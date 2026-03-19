use std::collections::HashMap;
use std::num::NonZeroU32;
use std::sync::Arc;

use anyhow::{Context, Result};
use bytemuck::{Pod, Zeroable};
use glyphon::{
    Attrs, Buffer, Cache, Color, Family, FontSystem, Metrics, Resolution, Shaping, SwashCache,
    TextArea, TextAtlas, TextBounds, TextRenderer, Viewport, Wrap,
};
use wgpu::util::DeviceExt;
use winit::dpi::PhysicalSize;
use winit::window::Window;

use super::bindings::BindingPromptState;
use super::ClientResource;
use super::LatencySnapshot;
use super::RenderState;

const SHADER_SOURCE: &str = r#"
struct Screen {
    virtual_size: vec2<f32>,
    _pad: vec2<f32>,
};

@group(0) @binding(0)
var<uniform> screen: Screen;

struct VsIn {
    @location(0) unit_pos: vec2<f32>,
    @location(1) center: vec2<f32>,
    @location(2) size: vec2<f32>,
    @location(3) color: vec4<f32>,
};

struct VsOut {
    @builtin(position) position: vec4<f32>,
    @location(0) color: vec4<f32>,
};

@vertex
fn vs_main(input: VsIn) -> VsOut {
    let world = input.center + input.unit_pos * input.size;
    let ndc = vec2<f32>(
        (world.x / screen.virtual_size.x) * 2.0 - 1.0,
        1.0 - (world.y / screen.virtual_size.y) * 2.0
    );

    var out: VsOut;
    out.position = vec4<f32>(ndc, 0.0, 1.0);
    out.color = input.color;
    return out;
}

@fragment
fn fs_main(input: VsOut) -> @location(0) vec4<f32> {
    return input.color;
}
"#;

const TEXTURED_SHADER_SOURCE: &str = r#"
struct Screen {
    virtual_size: vec2<f32>,
    _pad: vec2<f32>,
};

@group(0) @binding(0)
var<uniform> screen: Screen;

@group(1) @binding(0)
var texture_sampler: sampler;

@group(1) @binding(1)
var texture_data: texture_2d<f32>;

struct VsIn {
    @location(0) unit_pos: vec2<f32>,
    @location(1) center: vec2<f32>,
    @location(2) size: vec2<f32>,
    @location(3) color: vec4<f32>,
};

struct VsOut {
    @builtin(position) position: vec4<f32>,
    @location(0) uv: vec2<f32>,
    @location(1) color: vec4<f32>,
};

@vertex
fn vs_main(input: VsIn) -> VsOut {
    let world = input.center + input.unit_pos * input.size;
    let ndc = vec2<f32>(
        (world.x / screen.virtual_size.x) * 2.0 - 1.0,
        1.0 - (world.y / screen.virtual_size.y) * 2.0
    );

    var out: VsOut;
    out.position = vec4<f32>(ndc, 0.0, 1.0);
    out.uv = input.unit_pos + vec2<f32>(0.5, 0.5);
    out.color = input.color;
    return out;
}

@fragment
fn fs_main(input: VsOut) -> @location(0) vec4<f32> {
    return textureSample(texture_data, texture_sampler, input.uv) * input.color;
}
"#;

#[repr(C)]
#[derive(Clone, Copy, Pod, Zeroable)]
struct Vertex {
    pos: [f32; 2],
}

#[repr(C)]
#[derive(Clone, Copy, Pod, Zeroable)]
struct InstanceRaw {
    center: [f32; 2],
    size: [f32; 2],
    color: [f32; 4],
}

#[repr(C)]
#[derive(Clone, Copy, Pod, Zeroable)]
struct ScreenUniform {
    virtual_size: [f32; 2],
    _pad: [f32; 2],
}

struct GpuTexture {
    _texture: wgpu::Texture,
    bind_group: wgpu::BindGroup,
}

const QUAD_VERTICES: [Vertex; 4] = [
    Vertex { pos: [-0.5, -0.5] },
    Vertex { pos: [0.5, -0.5] },
    Vertex { pos: [0.5, 0.5] },
    Vertex { pos: [-0.5, 0.5] },
];

const QUAD_INDICES: [u16; 6] = [0, 1, 2, 2, 3, 0];

#[derive(Clone, Debug, PartialEq)]
pub(super) struct TextCommand {
    pub(super) text: String,
    pub(super) x: f32,
    pub(super) y: f32,
    pub(super) max_width: f32,
    pub(super) font_size: f32,
    pub(super) line_height: f32,
    pub(super) color: u32,
}

#[derive(Default)]
pub(super) struct OverlayFrame {
    pub(super) rects: Vec<RenderState>,
    pub(super) texts: Vec<TextCommand>,
}

impl OverlayFrame {
    pub(super) fn merge_into(self, rects: &mut Vec<RenderState>, texts: &mut Vec<TextCommand>) {
        rects.extend(self.rects);
        texts.extend(self.texts);
    }
}

pub(super) struct Renderer {
    surface: wgpu::Surface<'static>,
    device: wgpu::Device,
    queue: wgpu::Queue,
    config: wgpu::SurfaceConfiguration,
    pipeline: wgpu::RenderPipeline,
    textured_pipeline: wgpu::RenderPipeline,
    size: PhysicalSize<u32>,
    vertex_buffer: wgpu::Buffer,
    index_buffer: wgpu::Buffer,
    instance_buffer: wgpu::Buffer,
    instance_capacity: usize,
    instance_staging: Vec<InstanceRaw>,
    textured_instance_buffer: wgpu::Buffer,
    textured_instance_capacity: usize,
    textured_instance_staging: Vec<InstanceRaw>,
    uniform_buffer: wgpu::Buffer,
    uniform_bind_group: wgpu::BindGroup,
    texture_bind_group_layout: wgpu::BindGroupLayout,
    texture_sampler: wgpu::Sampler,
    textures: HashMap<u128, GpuTexture>,
    virtual_dimension_lock: Option<(u32, u32)>,
    aspect_ratio_lock: Option<(u32, u32)>,
    clear_color: wgpu::Color,
    font_system: FontSystem,
    swash_cache: SwashCache,
    viewport: Viewport,
    text_atlas: TextAtlas,
    text_renderer: TextRenderer,
    text_buffers: Vec<Buffer>,
}

impl Renderer {
    pub(super) async fn new(window: Arc<Window>) -> Result<Self> {
        let size = window.inner_size();

        let instance = wgpu::Instance::default();
        let surface =
            instance.create_surface(window.clone()).context("failed to create WGPU surface")?;

        let adapter = instance
            .request_adapter(&wgpu::RequestAdapterOptions {
                power_preference: wgpu::PowerPreference::HighPerformance,
                compatible_surface: Some(&surface),
                force_fallback_adapter: false,
            })
            .await
            .context("failed to request WGPU adapter")?;

        let (device, queue) = adapter
            .request_device(&wgpu::DeviceDescriptor {
                label: Some("widev-device"),
                required_features: wgpu::Features::empty(),
                required_limits: wgpu::Limits::default(),
                experimental_features: Default::default(),
                memory_hints: wgpu::MemoryHints::Performance,
                trace: wgpu::Trace::default(),
            })
            .await
            .context("failed to request WGPU device")?;

        let capabilities = surface.get_capabilities(&adapter);
        let format = capabilities
            .formats
            .iter()
            .copied()
            .find(wgpu::TextureFormat::is_srgb)
            .or_else(|| capabilities.formats.first().copied())
            .context("no supported surface format")?;

        let config = wgpu::SurfaceConfiguration {
            usage: wgpu::TextureUsages::RENDER_ATTACHMENT,
            format,
            width: size.width.max(1),
            height: size.height.max(1),
            present_mode: wgpu::PresentMode::Fifo,
            alpha_mode: capabilities.alpha_modes[0],
            view_formats: vec![],
            desired_maximum_frame_latency: 2,
        };
        surface.configure(&device, &config);

        let shader = device.create_shader_module(wgpu::ShaderModuleDescriptor {
            label: Some("widev-shader"),
            source: wgpu::ShaderSource::Wgsl(SHADER_SOURCE.into()),
        });
        let textured_shader = device.create_shader_module(wgpu::ShaderModuleDescriptor {
            label: Some("widev-textured-shader"),
            source: wgpu::ShaderSource::Wgsl(TEXTURED_SHADER_SOURCE.into()),
        });

        let uniform = ScreenUniform {
            virtual_size: [config.width as f32, config.height as f32],
            _pad: [0.0, 0.0],
        };
        let uniform_buffer = device.create_buffer_init(&wgpu::util::BufferInitDescriptor {
            label: Some("screen-uniform"),
            contents: bytemuck::bytes_of(&uniform),
            usage: wgpu::BufferUsages::UNIFORM | wgpu::BufferUsages::COPY_DST,
        });
        let uniform_layout = device.create_bind_group_layout(&wgpu::BindGroupLayoutDescriptor {
            label: Some("screen-uniform-layout"),
            entries: &[wgpu::BindGroupLayoutEntry {
                binding: 0,
                visibility: wgpu::ShaderStages::VERTEX,
                ty: wgpu::BindingType::Buffer {
                    ty: wgpu::BufferBindingType::Uniform,
                    has_dynamic_offset: false,
                    min_binding_size: None,
                },
                count: None,
            }],
        });
        let uniform_bind_group = device.create_bind_group(&wgpu::BindGroupDescriptor {
            label: Some("screen-uniform-bind-group"),
            layout: &uniform_layout,
            entries: &[wgpu::BindGroupEntry {
                binding: 0,
                resource: uniform_buffer.as_entire_binding(),
            }],
        });
        let texture_bind_group_layout =
            device.create_bind_group_layout(&wgpu::BindGroupLayoutDescriptor {
                label: Some("texture-bind-group-layout"),
                entries: &[
                    wgpu::BindGroupLayoutEntry {
                        binding: 0,
                        visibility: wgpu::ShaderStages::FRAGMENT,
                        ty: wgpu::BindingType::Sampler(wgpu::SamplerBindingType::Filtering),
                        count: None,
                    },
                    wgpu::BindGroupLayoutEntry {
                        binding: 1,
                        visibility: wgpu::ShaderStages::FRAGMENT,
                        ty: wgpu::BindingType::Texture {
                            sample_type: wgpu::TextureSampleType::Float { filterable: true },
                            view_dimension: wgpu::TextureViewDimension::D2,
                            multisampled: false,
                        },
                        count: None,
                    },
                ],
            });
        let texture_sampler = device.create_sampler(&wgpu::SamplerDescriptor {
            label: Some("texture-sampler"),
            mag_filter: wgpu::FilterMode::Nearest,
            min_filter: wgpu::FilterMode::Nearest,
            mipmap_filter: wgpu::MipmapFilterMode::Nearest,
            address_mode_u: wgpu::AddressMode::ClampToEdge,
            address_mode_v: wgpu::AddressMode::ClampToEdge,
            address_mode_w: wgpu::AddressMode::ClampToEdge,
            ..Default::default()
        });

        let pipeline_layout = device.create_pipeline_layout(&wgpu::PipelineLayoutDescriptor {
            label: Some("widev-pipeline-layout"),
            bind_group_layouts: &[&uniform_layout],
            immediate_size: 0,
        });
        let pipeline = device.create_render_pipeline(&wgpu::RenderPipelineDescriptor {
            label: Some("widev-pipeline"),
            layout: Some(&pipeline_layout),
            vertex: wgpu::VertexState {
                module: &shader,
                entry_point: Some("vs_main"),
                compilation_options: wgpu::PipelineCompilationOptions::default(),
                buffers: &[
                    wgpu::VertexBufferLayout {
                        array_stride: std::mem::size_of::<Vertex>() as u64,
                        step_mode: wgpu::VertexStepMode::Vertex,
                        attributes: &[wgpu::VertexAttribute {
                            offset: 0,
                            shader_location: 0,
                            format: wgpu::VertexFormat::Float32x2,
                        }],
                    },
                    wgpu::VertexBufferLayout {
                        array_stride: std::mem::size_of::<InstanceRaw>() as u64,
                        step_mode: wgpu::VertexStepMode::Instance,
                        attributes: &[
                            wgpu::VertexAttribute {
                                offset: 0,
                                shader_location: 1,
                                format: wgpu::VertexFormat::Float32x2,
                            },
                            wgpu::VertexAttribute {
                                offset: 8,
                                shader_location: 2,
                                format: wgpu::VertexFormat::Float32x2,
                            },
                            wgpu::VertexAttribute {
                                offset: 16,
                                shader_location: 3,
                                format: wgpu::VertexFormat::Float32x4,
                            },
                        ],
                    },
                ],
            },
            fragment: Some(wgpu::FragmentState {
                module: &shader,
                entry_point: Some("fs_main"),
                compilation_options: wgpu::PipelineCompilationOptions::default(),
                targets: &[Some(wgpu::ColorTargetState {
                    format: config.format,
                    blend: Some(wgpu::BlendState::ALPHA_BLENDING),
                    write_mask: wgpu::ColorWrites::ALL,
                })],
            }),
            primitive: wgpu::PrimitiveState::default(),
            depth_stencil: None,
            multisample: wgpu::MultisampleState::default(),
            multiview_mask: None,
            cache: None,
        });
        let textured_pipeline_layout =
            device.create_pipeline_layout(&wgpu::PipelineLayoutDescriptor {
                label: Some("widev-textured-pipeline-layout"),
                bind_group_layouts: &[&uniform_layout, &texture_bind_group_layout],
                immediate_size: 0,
            });
        let textured_pipeline = device.create_render_pipeline(&wgpu::RenderPipelineDescriptor {
            label: Some("widev-textured-pipeline"),
            layout: Some(&textured_pipeline_layout),
            vertex: wgpu::VertexState {
                module: &textured_shader,
                entry_point: Some("vs_main"),
                compilation_options: wgpu::PipelineCompilationOptions::default(),
                buffers: &[
                    wgpu::VertexBufferLayout {
                        array_stride: std::mem::size_of::<Vertex>() as u64,
                        step_mode: wgpu::VertexStepMode::Vertex,
                        attributes: &[wgpu::VertexAttribute {
                            offset: 0,
                            shader_location: 0,
                            format: wgpu::VertexFormat::Float32x2,
                        }],
                    },
                    wgpu::VertexBufferLayout {
                        array_stride: std::mem::size_of::<InstanceRaw>() as u64,
                        step_mode: wgpu::VertexStepMode::Instance,
                        attributes: &[
                            wgpu::VertexAttribute {
                                offset: 0,
                                shader_location: 1,
                                format: wgpu::VertexFormat::Float32x2,
                            },
                            wgpu::VertexAttribute {
                                offset: 8,
                                shader_location: 2,
                                format: wgpu::VertexFormat::Float32x2,
                            },
                            wgpu::VertexAttribute {
                                offset: 16,
                                shader_location: 3,
                                format: wgpu::VertexFormat::Float32x4,
                            },
                        ],
                    },
                ],
            },
            fragment: Some(wgpu::FragmentState {
                module: &textured_shader,
                entry_point: Some("fs_main"),
                compilation_options: wgpu::PipelineCompilationOptions::default(),
                targets: &[Some(wgpu::ColorTargetState {
                    format: config.format,
                    blend: Some(wgpu::BlendState::ALPHA_BLENDING),
                    write_mask: wgpu::ColorWrites::ALL,
                })],
            }),
            primitive: wgpu::PrimitiveState::default(),
            depth_stencil: None,
            multisample: wgpu::MultisampleState::default(),
            multiview_mask: None,
            cache: None,
        });

        let vertex_buffer = device.create_buffer_init(&wgpu::util::BufferInitDescriptor {
            label: Some("quad-vertex-buffer"),
            contents: bytemuck::cast_slice(&QUAD_VERTICES),
            usage: wgpu::BufferUsages::VERTEX,
        });
        let index_buffer = device.create_buffer_init(&wgpu::util::BufferInitDescriptor {
            label: Some("quad-index-buffer"),
            contents: bytemuck::cast_slice(&QUAD_INDICES),
            usage: wgpu::BufferUsages::INDEX,
        });
        let instance_buffer = device.create_buffer(&wgpu::BufferDescriptor {
            label: Some("instance-buffer"),
            size: std::mem::size_of::<InstanceRaw>() as u64,
            usage: wgpu::BufferUsages::VERTEX | wgpu::BufferUsages::COPY_DST,
            mapped_at_creation: false,
        });
        let textured_instance_buffer = device.create_buffer(&wgpu::BufferDescriptor {
            label: Some("textured-instance-buffer"),
            size: std::mem::size_of::<InstanceRaw>() as u64,
            usage: wgpu::BufferUsages::VERTEX | wgpu::BufferUsages::COPY_DST,
            mapped_at_creation: false,
        });

        let text_cache = Cache::new(&device);
        let mut text_atlas = TextAtlas::new(&device, &queue, &text_cache, format);
        let text_renderer =
            TextRenderer::new(&mut text_atlas, &device, wgpu::MultisampleState::default(), None);
        let viewport = Viewport::new(&device, &text_cache);

        Ok(Self {
            surface,
            device,
            queue,
            config,
            pipeline,
            textured_pipeline,
            size,
            vertex_buffer,
            index_buffer,
            instance_buffer,
            instance_capacity: 1,
            instance_staging: Vec::new(),
            textured_instance_buffer,
            textured_instance_capacity: 1,
            textured_instance_staging: Vec::new(),
            uniform_buffer,
            uniform_bind_group,
            texture_bind_group_layout,
            texture_sampler,
            textures: HashMap::new(),
            virtual_dimension_lock: None,
            aspect_ratio_lock: None,
            clear_color: wgpu::Color::BLACK,
            font_system: FontSystem::new(),
            swash_cache: SwashCache::new(),
            viewport,
            text_atlas,
            text_renderer,
            text_buffers: Vec::new(),
        })
    }

    pub(super) fn resize(&mut self, new_size: PhysicalSize<u32>) {
        if new_size.width == 0 || new_size.height == 0 {
            return;
        }

        self.size = new_size;
        self.config.width = new_size.width;
        self.config.height = new_size.height;
        self.surface.configure(&self.device, &self.config);
        self.write_screen_uniform();
    }

    pub(super) fn set_surface_constraints(
        &mut self,
        dimension_lock: Option<(u32, u32)>,
        aspect_ratio_lock: Option<(u32, u32)>,
        clear_background_oklch: Option<[f32; 4]>,
    ) {
        if let Some(color) = clear_background_oklch {
            self.clear_color = oklch_to_clear_color(color);
        }
        if self.virtual_dimension_lock == dimension_lock
            && self.aspect_ratio_lock == aspect_ratio_lock
        {
            return;
        }
        self.virtual_dimension_lock = dimension_lock;
        self.aspect_ratio_lock = aspect_ratio_lock;
        self.write_screen_uniform();
    }

    pub(super) fn render(
        &mut self,
        render_states: &[RenderState],
        resources: &HashMap<u128, ClientResource>,
        text_commands: &[TextCommand],
    ) -> Result<()> {
        self.sync_textures(resources)?;
        let (colored_states, textured_states): (Vec<_>, Vec<_>) =
            render_states.iter().partition(|state| state.texture_id.is_none());
        self.ensure_instance_capacity(colored_states.len().max(1));
        self.ensure_textured_instance_capacity(textured_states.len().max(1));
        self.write_instances(&colored_states);
        self.write_textured_instances(&textured_states);

        let frame = match self.surface.get_current_texture() {
            Ok(frame) => frame,
            Err(wgpu::SurfaceError::Lost) => {
                self.resize(self.size);
                return Ok(());
            },
            Err(wgpu::SurfaceError::Outdated) => return Ok(()),
            Err(wgpu::SurfaceError::Timeout) => return Ok(()),
            Err(wgpu::SurfaceError::Other) => return Ok(()),
            Err(wgpu::SurfaceError::OutOfMemory) => {
                return Err(anyhow::anyhow!("wgpu surface out of memory"));
            },
        };
        let view = frame.texture.create_view(&wgpu::TextureViewDescriptor::default());
        let (vp_x, vp_y, vp_w, vp_h) = self.compute_viewport();
        self.viewport.update(
            &self.queue,
            Resolution { width: self.config.width, height: self.config.height },
        );
        self.prepare_text(text_commands, vp_x, vp_y, vp_w, vp_h)
            .context("failed to prepare text")?;

        let mut encoder = self
            .device
            .create_command_encoder(&wgpu::CommandEncoderDescriptor { label: Some("encoder") });
        {
            let mut pass = encoder.begin_render_pass(&wgpu::RenderPassDescriptor {
                label: Some("main-pass"),
                color_attachments: &[Some(wgpu::RenderPassColorAttachment {
                    view: &view,
                    depth_slice: None,
                    resolve_target: None,
                    ops: wgpu::Operations {
                        load: wgpu::LoadOp::Clear(self.clear_color),
                        store: wgpu::StoreOp::Store,
                    },
                })],
                depth_stencil_attachment: None,
                occlusion_query_set: None,
                timestamp_writes: None,
                multiview_mask: None,
            });

            pass.set_pipeline(&self.pipeline);
            pass.set_bind_group(0, &self.uniform_bind_group, &[]);
            pass.set_vertex_buffer(0, self.vertex_buffer.slice(..));
            pass.set_vertex_buffer(1, self.instance_buffer.slice(..));
            pass.set_index_buffer(self.index_buffer.slice(..), wgpu::IndexFormat::Uint16);
            pass.set_viewport(vp_x as f32, vp_y as f32, vp_w as f32, vp_h as f32, 0.0, 1.0);
            pass.set_scissor_rect(vp_x, vp_y, vp_w, vp_h);
            pass.draw_indexed(0..QUAD_INDICES.len() as u32, 0, 0..colored_states.len() as u32);
            if !textured_states.is_empty() {
                pass.set_pipeline(&self.textured_pipeline);
                pass.set_bind_group(0, &self.uniform_bind_group, &[]);
                pass.set_vertex_buffer(0, self.vertex_buffer.slice(..));
                pass.set_vertex_buffer(1, self.textured_instance_buffer.slice(..));
                pass.set_index_buffer(self.index_buffer.slice(..), wgpu::IndexFormat::Uint16);
                pass.set_viewport(vp_x as f32, vp_y as f32, vp_w as f32, vp_h as f32, 0.0, 1.0);
                pass.set_scissor_rect(vp_x, vp_y, vp_w, vp_h);
                for (index, state) in textured_states.iter().enumerate() {
                    let Some(texture_id) = state.texture_id else {
                        continue;
                    };
                    let Some(texture) = self.textures.get(&texture_id) else {
                        continue;
                    };
                    pass.set_bind_group(1, &texture.bind_group, &[]);
                    pass.draw_indexed(
                        0..QUAD_INDICES.len() as u32,
                        0,
                        index as u32..index as u32 + 1,
                    );
                }
            }
            self.text_renderer
                .render(&self.text_atlas, &self.viewport, &mut pass)
                .context("failed to render text")?;
        }

        self.queue.submit(std::iter::once(encoder.finish()));
        frame.present();
        self.text_atlas.trim();
        Ok(())
    }

    pub(super) fn build_binding_overlay(&self, prompt: &BindingPromptState) -> OverlayFrame {
        let mut overlay = OverlayFrame::default();
        let (virtual_w, virtual_h) = self.compute_virtual_size();
        let vw = virtual_w as f32;
        let vh = virtual_h as f32;

        let ui_scale = ((vw.min(vh) / 720.0).clamp(0.9, 1.35) * 100.0).round() / 100.0;
        let outer_margin = (32.0 * ui_scale).max(18.0);
        let panel_w = (vw * 0.72).clamp(520.0, (vw - outer_margin * 2.0).max(520.0));
        let panel_h = (vh * 0.74).clamp(360.0, (vh - outer_margin * 2.0).max(360.0));
        let panel_left = ((vw - panel_w) * 0.5).max(outer_margin);
        let panel_top = ((vh - panel_h) * 0.5).max(outer_margin);
        let pad = 24.0 * ui_scale;
        let section_gap = 18.0 * ui_scale;
        let card_gap = 14.0 * ui_scale;
        let left_col_w = (panel_w * 0.36).clamp(180.0, 280.0);
        let right_col_w = panel_w - pad * 2.0 - left_col_w - card_gap;
        let top_y = panel_top + pad;
        let bottom_y = panel_top + panel_h - pad;

        let hero_h = 92.0 * ui_scale;
        let info_h = 110.0 * ui_scale;
        let footer_h = 64.0 * ui_scale;
        let right_h = bottom_y - top_y;
        let left_h = hero_h + section_gap + info_h + section_gap + footer_h;
        let left_top = top_y + ((right_h - left_h).max(0.0) * 0.5);
        let right_top = top_y;
        let left_x = panel_left + pad;
        let right_x = left_x + left_col_w + card_gap;

        let hero_top = left_top;
        let info_top = hero_top + hero_h + section_gap;
        let footer_top = info_top + info_h + section_gap;
        let capture_h = right_h * 0.42;
        let capture_top = right_top;
        let supported_top = capture_top + capture_h + section_gap;
        let supported_h = bottom_y - supported_top;

        let panel_fill = 0x0b1017;
        let panel_border = 0x293242;
        let card_fill = 0x111823;
        let card_fill_emphasis = 0x151f2d;
        let divider = 0x223042;
        let text_primary = 0xe5e7eb;
        let text_muted = 0x94a3b8;
        let text_soft = 0xcbd5e1;
        let accent = 0xcbd5e1;
        let accent_warm = 0xf0d28a;
        let accent_good = 0xa7d8b5;

        push_rect(&mut overlay.rects, panel_left, panel_top, panel_w, panel_h, panel_fill);
        draw_border(&mut overlay.rects, panel_left, panel_top, panel_w, panel_h, panel_border);

        for (left, top, width, height, fill) in [
            (left_x, hero_top, left_col_w, hero_h, card_fill_emphasis),
            (left_x, info_top, left_col_w, info_h, card_fill),
            (left_x, footer_top, left_col_w, footer_h, card_fill),
            (right_x, capture_top, right_col_w, capture_h, card_fill_emphasis),
            (right_x, supported_top, right_col_w, supported_h, card_fill),
        ] {
            push_rect(&mut overlay.rects, left, top, width, height, fill);
            draw_border(&mut overlay.rects, left, top, width, height, divider);
        }

        let section_pad = 18.0 * ui_scale;
        let label_size = 11.0 * ui_scale;
        let body_size = 14.0 * ui_scale;
        let title_size = 27.0 * ui_scale;
        let value_size = 18.0 * ui_scale;
        let small_size = 12.0 * ui_scale;

        draw_text_line(
            &mut overlay.texts,
            left_x + section_pad,
            hero_top + 18.0 * ui_scale,
            "INPUT BINDING",
            left_col_w - section_pad * 2.0,
            label_size,
            text_muted,
        );
        draw_text_line(
            &mut overlay.texts,
            left_x + section_pad,
            hero_top + 40.0 * ui_scale,
            &prompt.identifier,
            left_col_w - section_pad * 2.0,
            title_size,
            text_primary,
        );
        draw_text_line(
            &mut overlay.texts,
            left_x + section_pad,
            hero_top + 70.0 * ui_scale,
            &format!("{:?}", prompt.input_type),
            left_col_w - section_pad * 2.0,
            body_size,
            accent,
        );

        let scope_label = if prompt.any_device_scope { "Any device (*)" } else { "Exact device" };
        let capture_label = prompt.suggestion.as_deref().unwrap_or("Waiting for input");

        draw_text_line(
            &mut overlay.texts,
            left_x + section_pad,
            info_top + 18.0 * ui_scale,
            "SCOPE",
            left_col_w - section_pad * 2.0,
            label_size,
            text_muted,
        );
        draw_text_line(
            &mut overlay.texts,
            left_x + section_pad,
            info_top + 40.0 * ui_scale,
            scope_label,
            left_col_w - section_pad * 2.0,
            value_size,
            accent_warm,
        );
        if let Some(hint) = &prompt.capture_hint {
            draw_text_line(
                &mut overlay.texts,
                left_x + section_pad,
                info_top + 72.0 * ui_scale,
                hint,
                left_col_w - section_pad * 2.0,
                small_size,
                text_soft,
            );
        }

        draw_text_line(
            &mut overlay.texts,
            left_x + section_pad,
            footer_top + 16.0 * ui_scale,
            "Enter confirm  |  Backspace skip  |  Tab scope  |  Esc close",
            left_col_w - section_pad * 2.0,
            small_size,
            text_soft,
        );

        draw_text_line(
            &mut overlay.texts,
            right_x + section_pad,
            capture_top + 18.0 * ui_scale,
            "CAPTURED INPUT",
            right_col_w - section_pad * 2.0,
            label_size,
            text_muted,
        );
        draw_text_line(
            &mut overlay.texts,
            right_x + section_pad,
            capture_top + 48.0 * ui_scale,
            capture_label,
            right_col_w - section_pad * 2.0,
            value_size,
            accent_good,
        );
        draw_divider(
            &mut overlay.rects,
            right_x + section_pad,
            capture_top + capture_h - 26.0 * ui_scale,
            right_col_w - section_pad * 2.0,
            divider,
        );
        draw_text_line(
            &mut overlay.texts,
            right_x + section_pad,
            capture_top + capture_h - 18.0 * ui_scale,
            "Press the control you want to bind. Confirm once the preview matches.",
            right_col_w - section_pad * 2.0,
            small_size,
            text_muted,
        );

        draw_text_line(
            &mut overlay.texts,
            right_x + section_pad,
            supported_top + 18.0 * ui_scale,
            "SUPPORTED INPUTS",
            right_col_w - section_pad * 2.0,
            label_size,
            text_muted,
        );
        let line_height = 22.0 * ui_scale;
        let mut y = supported_top + 48.0 * ui_scale;
        let max_y = supported_top + supported_h - section_pad;
        for line in friendly_supported_lines(prompt) {
            if y + line_height > max_y {
                break;
            }
            draw_text_line(
                &mut overlay.texts,
                right_x + section_pad,
                y,
                &format!("• {line}"),
                right_col_w - section_pad * 2.0,
                body_size,
                text_soft,
            );
            y += line_height;
        }

        overlay
    }

    pub(super) fn build_latency_overlay(&self, latency: LatencySnapshot) -> OverlayFrame {
        let mut overlay = OverlayFrame::default();
        let (virtual_w, _virtual_h) = self.compute_virtual_size();
        let vw = virtual_w as f32;

        let horizontal_margin = 12.0;
        let vertical_margin = 10.0;

        let path_text = if latency.connected {
            format_latency_ms(latency.quiche_rtt.map(|v| v.as_secs_f64() * 1000.0))
        } else {
            "--".to_string()
        };
        let font_size = 24.0;
        let text_width = estimate_text_width(&path_text, font_size);
        let left = (vw - horizontal_margin - text_width).max(0.0);
        let top = vertical_margin;

        draw_text_line(&mut overlay.texts, left, top, &path_text, text_width, font_size, 0xffffff);

        overlay
    }

    fn ensure_instance_capacity(&mut self, required: usize) {
        if required <= self.instance_capacity {
            return;
        }
        self.instance_capacity = required.next_power_of_two();
        self.instance_buffer = self.device.create_buffer(&wgpu::BufferDescriptor {
            label: Some("instance-buffer"),
            size: (self.instance_capacity * std::mem::size_of::<InstanceRaw>()) as u64,
            usage: wgpu::BufferUsages::VERTEX | wgpu::BufferUsages::COPY_DST,
            mapped_at_creation: false,
        });
    }

    fn ensure_textured_instance_capacity(&mut self, required: usize) {
        if required <= self.textured_instance_capacity {
            return;
        }
        self.textured_instance_capacity = required.next_power_of_two();
        self.textured_instance_buffer = self.device.create_buffer(&wgpu::BufferDescriptor {
            label: Some("textured-instance-buffer"),
            size: (self.textured_instance_capacity * std::mem::size_of::<InstanceRaw>()) as u64,
            usage: wgpu::BufferUsages::VERTEX | wgpu::BufferUsages::COPY_DST,
            mapped_at_creation: false,
        });
    }

    fn write_instances(&mut self, render_states: &[&RenderState]) {
        if render_states.is_empty() {
            return;
        }
        self.instance_staging.clear();
        self.instance_staging.extend(render_states.iter().map(|state| InstanceRaw {
            // Game-space positions are top-left corners; the quad geometry is
            // centered, so convert to a center point here.
            center: [state.x + state.width * 0.5, state.y + state.height * 0.5],
            size: [state.width, state.height],
            color: unpack_color(state.color),
        }));
        self.queue.write_buffer(
            &self.instance_buffer,
            0,
            bytemuck::cast_slice(&self.instance_staging),
        );
    }

    fn write_textured_instances(&mut self, render_states: &[&RenderState]) {
        if render_states.is_empty() {
            return;
        }
        self.textured_instance_staging.clear();
        self.textured_instance_staging.extend(render_states.iter().map(|state| InstanceRaw {
            // Game-space positions are top-left corners; the quad geometry is
            // centered, so convert to a center point here.
            center: [state.x + state.width * 0.5, state.y + state.height * 0.5],
            size: [state.width, state.height],
            color: unpack_color(state.color),
        }));
        self.queue.write_buffer(
            &self.textured_instance_buffer,
            0,
            bytemuck::cast_slice(&self.textured_instance_staging),
        );
    }

    fn sync_textures(&mut self, resources: &HashMap<u128, ClientResource>) -> Result<()> {
        self.textures.retain(|texture_id, _| {
            resources.get(texture_id).and_then(ClientResource::texture).is_some()
        });

        for (&texture_id, resource) in resources {
            let Some(texture) = resource.texture() else {
                continue;
            };
            if self.textures.contains_key(&texture_id) {
                continue;
            }
            let size = wgpu::Extent3d {
                width: texture.width.max(1),
                height: texture.height.max(1),
                depth_or_array_layers: 1,
            };
            let gpu_texture = self.device.create_texture(&wgpu::TextureDescriptor {
                label: Some("element-texture"),
                size,
                mip_level_count: 1,
                sample_count: 1,
                dimension: wgpu::TextureDimension::D2,
                format: wgpu::TextureFormat::Rgba8UnormSrgb,
                usage: wgpu::TextureUsages::TEXTURE_BINDING | wgpu::TextureUsages::COPY_DST,
                view_formats: &[],
            });
            self.queue.write_texture(
                wgpu::TexelCopyTextureInfo {
                    texture: &gpu_texture,
                    mip_level: 0,
                    origin: wgpu::Origin3d::ZERO,
                    aspect: wgpu::TextureAspect::All,
                },
                &texture.rgba,
                wgpu::TexelCopyBufferLayout {
                    offset: 0,
                    bytes_per_row: Some(
                        NonZeroU32::new(texture.width.max(1) * 4)
                            .expect("texture row bytes must be non-zero")
                            .into(),
                    ),
                    rows_per_image: Some(
                        NonZeroU32::new(texture.height.max(1))
                            .expect("texture height must be non-zero")
                            .into(),
                    ),
                },
                size,
            );
            let view = gpu_texture.create_view(&wgpu::TextureViewDescriptor::default());
            let bind_group = self.device.create_bind_group(&wgpu::BindGroupDescriptor {
                label: Some("element-texture-bind-group"),
                layout: &self.texture_bind_group_layout,
                entries: &[
                    wgpu::BindGroupEntry {
                        binding: 0,
                        resource: wgpu::BindingResource::Sampler(&self.texture_sampler),
                    },
                    wgpu::BindGroupEntry {
                        binding: 1,
                        resource: wgpu::BindingResource::TextureView(&view),
                    },
                ],
            });
            self.textures.insert(texture_id, GpuTexture { _texture: gpu_texture, bind_group });
        }
        Ok(())
    }

    fn write_screen_uniform(&mut self) {
        let (virtual_width, virtual_height) = self.compute_virtual_size();
        let uniform = ScreenUniform {
            virtual_size: [virtual_width as f32, virtual_height as f32],
            _pad: [0.0, 0.0],
        };
        self.queue.write_buffer(&self.uniform_buffer, 0, bytemuck::bytes_of(&uniform));
    }

    fn compute_virtual_size(&self) -> (u32, u32) {
        if let Some((width, height)) = self.virtual_dimension_lock {
            if width > 0 && height > 0 {
                return (width, height);
            }
        }
        if let Some((numerator, denominator)) = self.aspect_ratio_lock {
            if numerator > 0 && denominator > 0 {
                return enforce_aspect(
                    self.config.width.max(1),
                    self.config.height.max(1),
                    numerator,
                    denominator,
                );
            }
        }
        (self.config.width.max(1), self.config.height.max(1))
    }

    fn compute_viewport(&self) -> (u32, u32, u32, u32) {
        let surface_w = self.config.width.max(1);
        let surface_h = self.config.height.max(1);
        let (virtual_w, virtual_h) = self.compute_virtual_size();

        let surface_ratio = surface_w as f64 / surface_h as f64;
        let virtual_ratio = virtual_w as f64 / virtual_h as f64;
        if (surface_ratio - virtual_ratio).abs() < f64::EPSILON {
            return (0, 0, surface_w, surface_h);
        }

        if surface_ratio > virtual_ratio {
            let vp_h = surface_h;
            let vp_w = ((vp_h as f64) * virtual_ratio).round().max(1.0) as u32;
            let vp_x = (surface_w.saturating_sub(vp_w)) / 2;
            (vp_x, 0, vp_w, vp_h)
        } else {
            let vp_w = surface_w;
            let vp_h = ((vp_w as f64) / virtual_ratio).round().max(1.0) as u32;
            let vp_y = (surface_h.saturating_sub(vp_h)) / 2;
            (0, vp_y, vp_w, vp_h)
        }
    }

    fn prepare_text(
        &mut self,
        text_commands: &[TextCommand],
        vp_x: u32,
        vp_y: u32,
        vp_w: u32,
        vp_h: u32,
    ) -> Result<()> {
        self.text_buffers.clear();
        let (virtual_w, virtual_h) = self.compute_virtual_size();
        let scale_x = vp_w as f32 / virtual_w.max(1) as f32;
        let scale_y = vp_h as f32 / virtual_h.max(1) as f32;

        for command in text_commands {
            let font_size = (command.font_size * scale_y).max(1.0).round();
            let line_height = (command.line_height * scale_y).max(font_size + 1.0).round();
            let mut buffer =
                Buffer::new(&mut self.font_system, Metrics::new(font_size, line_height));
            buffer.set_wrap(&mut self.font_system, Wrap::None);
            buffer.set_size(
                &mut self.font_system,
                Some((command.max_width * scale_x).max(1.0).round()),
                None,
            );
            buffer.set_text(
                &mut self.font_system,
                &command.text,
                &Attrs::new().family(Family::SansSerif),
                Shaping::Basic,
                None,
            );
            buffer.shape_until_scroll(&mut self.font_system, false);
            self.text_buffers.push(buffer);
        }

        let text_areas: Vec<TextArea<'_>> = self
            .text_buffers
            .iter()
            .zip(text_commands.iter())
            .map(|(buffer, command)| TextArea {
                buffer,
                left: (vp_x as f32 + command.x * scale_x).round(),
                top: (vp_y as f32 + command.y * scale_y).round(),
                scale: 1.0,
                bounds: TextBounds {
                    left: vp_x as i32,
                    top: vp_y as i32,
                    right: (vp_x + vp_w) as i32,
                    bottom: (vp_y + vp_h) as i32,
                },
                default_color: text_color(command.color),
                custom_glyphs: &[],
            })
            .collect();

        self.text_renderer.prepare(
            &self.device,
            &self.queue,
            &mut self.font_system,
            &mut self.text_atlas,
            &self.viewport,
            text_areas,
            &mut self.swash_cache,
        )?;
        Ok(())
    }
}

fn unpack_color(rgb: u32) -> [f32; 4] {
    let r = ((rgb >> 16) & 0xff) as f32 / 255.0;
    let g = ((rgb >> 8) & 0xff) as f32 / 255.0;
    let b = (rgb & 0xff) as f32 / 255.0;
    [r, g, b, 1.0]
}

fn enforce_aspect(width: u32, height: u32, numerator: u32, denominator: u32) -> (u32, u32) {
    let width = width.max(1);
    let height = height.max(1);
    let target_height = ((width as u64 * denominator as u64) / numerator as u64).max(1) as u32;
    let target_width = ((height as u64 * numerator as u64) / denominator as u64).max(1) as u32;
    let delta_h = (target_height as i64 - height as i64).abs();
    let delta_w = (target_width as i64 - width as i64).abs();
    if delta_h <= delta_w {
        (width, target_height)
    } else {
        (target_width, height)
    }
}

fn oklch_to_clear_color([l, c, h_deg, alpha]: [f32; 4]) -> wgpu::Color {
    let l = l.clamp(0.0, 1.0) as f64;
    let c = c.max(0.0) as f64;
    let hue = (h_deg as f64).to_radians();
    let a = c * hue.cos();
    let b = c * hue.sin();

    let l_ = l + 0.396_337_777_4 * a + 0.215_803_757_3 * b;
    let m_ = l - 0.105_561_345_8 * a - 0.063_854_172_8 * b;
    let s_ = l - 0.089_484_177_5 * a - 1.291_485_548 * b;

    let l3 = l_ * l_ * l_;
    let m3 = m_ * m_ * m_;
    let s3 = s_ * s_ * s_;

    let r = (4.076_741_662_1 * l3 - 3.307_711_591_3 * m3 + 0.230_969_929_2 * s3).clamp(0.0, 1.0);
    let g = (-1.268_438_004_6 * l3 + 2.609_757_401_1 * m3 - 0.341_319_396_5 * s3).clamp(0.0, 1.0);
    let b = (-0.004_196_086_3 * l3 - 0.703_418_614_7 * m3 + 1.707_614_701 * s3).clamp(0.0, 1.0);

    wgpu::Color { r, g, b, a: alpha.clamp(0.0, 1.0) as f64 }
}

fn push_rect(states: &mut Vec<RenderState>, x: f32, y: f32, width: f32, height: f32, color: u32) {
    let sx = x.round();
    let sy = y.round();
    let sw = width.max(1.0).round();
    let sh = height.max(1.0).round();
    states.push(RenderState { x: sx, y: sy, width: sw, height: sh, color, texture_id: None });
}

fn draw_border(
    states: &mut Vec<RenderState>,
    left: f32,
    top: f32,
    width: f32,
    height: f32,
    color: u32,
) {
    let t = 1.0;
    push_rect(states, left, top, width, t, color);
    push_rect(states, left, top + height - t, width, t, color);
    push_rect(states, left, top, t, height, color);
    push_rect(states, left + width - t, top, t, height, color);
}

fn draw_divider(states: &mut Vec<RenderState>, left: f32, top: f32, width: f32, color: u32) {
    push_rect(states, left, top, width, 1.0, color);
}

fn draw_text_line(
    texts: &mut Vec<TextCommand>,
    x: f32,
    y: f32,
    text: &str,
    max_width: f32,
    font_size: f32,
    color: u32,
) {
    texts.push(TextCommand {
        text: text.to_string(),
        x: x.round(),
        y: y.round(),
        max_width: max_width.max(1.0),
        font_size: font_size.max(1.0),
        line_height: (font_size * 1.2).max(font_size + 2.0),
        color,
    });
}

fn format_latency_ms(value_ms: Option<f64>) -> String {
    match value_ms {
        Some(ms) if ms.is_finite() => format!("{} ms", ms.round() as i64),
        _ => "--".to_string(),
    }
}

fn estimate_text_width(text: &str, font_size: f32) -> f32 {
    (text.chars().count() as f32 * font_size * 0.62).ceil().max(font_size)
}

fn text_color(rgb: u32) -> Color {
    let r = ((rgb >> 16) & 0xff) as u8;
    let g = ((rgb >> 8) & 0xff) as u8;
    let b = (rgb & 0xff) as u8;
    Color::rgb(r, g, b)
}

fn friendly_supported_lines(prompt: &BindingPromptState) -> Vec<String> {
    let mut lines = Vec::new();
    lines.push(format!("Input type: {:?}", prompt.input_type));
    lines.push(format!(
        "Device scope: {}",
        if prompt.any_device_scope { "Any device (*)" } else { "Exact device only" }
    ));
    if prompt.allows_toggle {
        lines.push("Keyboard keys, mouse buttons, and gamepad buttons".to_string());
    }
    if prompt.allows_axis {
        lines.push("Mouse wheel/motion and gamepad analog axes".to_string());
    }
    if prompt.allows_joystick {
        lines.push("Gamepad sticks and keyboard virtual sticks".to_string());
    }
    if let Some(hint) = &prompt.capture_hint {
        lines.push(hint.clone());
    }
    lines.push("Tab switches between exact device and wildcard (*) scope".to_string());
    lines
}
