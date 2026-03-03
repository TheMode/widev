use std::sync::Arc;

use anyhow::{Context, Result};
use bytemuck::{Pod, Zeroable};
use taffy::prelude::*;
use wgpu::util::DeviceExt;
use winit::dpi::PhysicalSize;
use winit::window::Window;

use super::bindings::BindingPromptState;
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

const QUAD_VERTICES: [Vertex; 4] = [
    Vertex { pos: [-0.5, -0.5] },
    Vertex { pos: [0.5, -0.5] },
    Vertex { pos: [0.5, 0.5] },
    Vertex { pos: [-0.5, 0.5] },
];

const QUAD_INDICES: [u16; 6] = [0, 1, 2, 2, 3, 0];

pub(super) struct Renderer {
    surface: wgpu::Surface<'static>,
    device: wgpu::Device,
    queue: wgpu::Queue,
    config: wgpu::SurfaceConfiguration,
    pipeline: wgpu::RenderPipeline,
    size: PhysicalSize<u32>,
    vertex_buffer: wgpu::Buffer,
    index_buffer: wgpu::Buffer,
    instance_buffer: wgpu::Buffer,
    instance_capacity: usize,
    instance_staging: Vec<InstanceRaw>,
    uniform_buffer: wgpu::Buffer,
    uniform_bind_group: wgpu::BindGroup,
    virtual_dimension_lock: Option<(u32, u32)>,
    aspect_ratio_lock: Option<(u32, u32)>,
    clear_color: wgpu::Color,
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

        let pipeline_layout = device.create_pipeline_layout(&wgpu::PipelineLayoutDescriptor {
            label: Some("widev-pipeline-layout"),
            bind_group_layouts: &[&uniform_layout],
            push_constant_ranges: &[],
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
            multiview: None,
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

        Ok(Self {
            surface,
            device,
            queue,
            config,
            pipeline,
            size,
            vertex_buffer,
            index_buffer,
            instance_buffer,
            instance_capacity: 1,
            instance_staging: Vec::new(),
            uniform_buffer,
            uniform_bind_group,
            virtual_dimension_lock: None,
            aspect_ratio_lock: None,
            clear_color: wgpu::Color::BLACK,
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

    pub(super) fn render(&mut self, render_states: &[RenderState]) -> Result<()> {
        self.ensure_instance_capacity(render_states.len().max(1));
        self.write_instances(render_states);

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
            });

            pass.set_pipeline(&self.pipeline);
            pass.set_bind_group(0, &self.uniform_bind_group, &[]);
            pass.set_vertex_buffer(0, self.vertex_buffer.slice(..));
            pass.set_vertex_buffer(1, self.instance_buffer.slice(..));
            pass.set_index_buffer(self.index_buffer.slice(..), wgpu::IndexFormat::Uint16);
            pass.set_viewport(vp_x as f32, vp_y as f32, vp_w as f32, vp_h as f32, 0.0, 1.0);
            pass.set_scissor_rect(vp_x, vp_y, vp_w, vp_h);
            pass.draw_indexed(0..QUAD_INDICES.len() as u32, 0, 0..render_states.len() as u32);
        }

        self.queue.submit(std::iter::once(encoder.finish()));
        frame.present();
        Ok(())
    }

    pub(super) fn build_binding_overlay(&self, prompt: &BindingPromptState) -> Vec<RenderState> {
        let mut states = Vec::new();
        let (virtual_w, virtual_h) = self.compute_virtual_size();
        let vw = virtual_w as f32;
        let vh = virtual_h as f32;

        let panel_w = (vw * 0.86).clamp(540.0, (vw - 24.0).max(540.0));
        let panel_h = (vh * 0.84).clamp(420.0, (vh - 24.0).max(420.0));
        let panel_x = vw * 0.5;
        let panel_y = vh * 0.5;
        let panel_left = panel_x - panel_w * 0.5;
        let panel_top = panel_y - panel_h * 0.5;
        let ui_scale = (panel_w / 760.0).clamp(1.0, 2.0).round();
        let pad = 20.0 * ui_scale;
        let gap = 14.0 * ui_scale;
        let header_h = 56.0 * ui_scale;
        let action_h = 72.0 * ui_scale;
        let row2_h = 84.0 * ui_scale;
        let controls_h = 68.0 * ui_scale;
        let mut taffy: TaffyTree<()> = TaffyTree::new();
        let (Ok(header), Ok(action), Ok(left_card), Ok(right_card), Ok(controls), Ok(sources)) = (
            taffy.new_leaf(Style {
                size: Size { width: percent(1.0), height: length(header_h) },
                ..Default::default()
            }),
            taffy.new_leaf(Style {
                size: Size { width: percent(1.0), height: length(action_h) },
                ..Default::default()
            }),
            taffy.new_leaf(Style {
                flex_grow: 1.0,
                flex_basis: length(0.0),
                size: Size { width: auto(), height: percent(1.0) },
                ..Default::default()
            }),
            taffy.new_leaf(Style {
                flex_grow: 1.0,
                flex_basis: length(0.0),
                size: Size { width: auto(), height: percent(1.0) },
                ..Default::default()
            }),
            taffy.new_leaf(Style {
                size: Size { width: percent(1.0), height: length(controls_h) },
                ..Default::default()
            }),
            taffy.new_leaf(Style {
                flex_grow: 1.0,
                min_size: Size { width: auto(), height: length(70.0 * ui_scale) },
                ..Default::default()
            }),
        ) else {
            return states;
        };
        let Ok(row2) = taffy.new_with_children(
            Style {
                display: Display::Flex,
                flex_direction: FlexDirection::Row,
                gap: Size { width: length(gap), height: length(0.0) },
                size: Size { width: percent(1.0), height: length(row2_h) },
                ..Default::default()
            },
            &[left_card, right_card],
        ) else {
            return states;
        };
        let Ok(root) = taffy.new_with_children(
            Style {
                display: Display::Flex,
                flex_direction: FlexDirection::Column,
                size: Size { width: length(panel_w), height: length(panel_h) },
                padding: Rect {
                    left: length(pad),
                    right: length(pad),
                    top: length(pad),
                    bottom: length(pad),
                },
                gap: Size { width: length(0.0), height: length(gap) },
                ..Default::default()
            },
            &[header, action, row2, controls, sources],
        ) else {
            return states;
        };
        if taffy
            .compute_layout(
                root,
                Size {
                    width: AvailableSpace::Definite(panel_w),
                    height: AvailableSpace::Definite(panel_h),
                },
            )
            .is_err()
        {
            return states;
        }

        let get_rect = |node| {
            let layout = taffy.layout(node).ok()?;
            let left = panel_left + layout.location.x;
            let top = panel_top + layout.location.y;
            Some((left, top, layout.size.width, layout.size.height))
        };
        let Some((header_left, header_top, header_w, header_h)) = get_rect(header) else {
            return states;
        };
        let Some((action_left, action_top, action_w, action_h)) = get_rect(action) else {
            return states;
        };
        let Some((row2_left, row2_top, _row2_w, _row2_h)) = get_rect(row2) else {
            return states;
        };
        let left_layout = match taffy.layout(left_card) {
            Ok(layout) => layout,
            Err(_) => return states,
        };
        let right_layout = match taffy.layout(right_card) {
            Ok(layout) => layout,
            Err(_) => return states,
        };
        let left_left = row2_left + left_layout.location.x;
        let left_top = row2_top + left_layout.location.y;
        let left_w = left_layout.size.width;
        let left_h = left_layout.size.height;
        let right_left = row2_left + right_layout.location.x;
        let right_top = row2_top + right_layout.location.y;
        let right_w = right_layout.size.width;
        let right_h = right_layout.size.height;
        if right_w <= 0.0 || right_h <= 0.0 {
            return states;
        }
        let Some((controls_left, controls_top, controls_w, controls_h)) = get_rect(controls) else {
            return states;
        };
        let Some((sources_left, sources_top, sources_w, sources_h)) = get_rect(sources) else {
            return states;
        };

        push_rect(&mut states, panel_x, panel_y, panel_w, panel_h, 0x070b12);
        draw_border(&mut states, panel_left, panel_top, panel_w, panel_h, 0x2b3f5f);
        push_rect(
            &mut states,
            header_left + header_w * 0.5,
            header_top + header_h * 0.5,
            header_w,
            header_h,
            0x142033,
        );
        draw_text_line(
            &mut states,
            header_left + 12.0 * ui_scale,
            header_top + 14.0 * ui_scale,
            "Input Binding",
            header_w - 24.0 * ui_scale,
            2.0 * ui_scale,
            0xe2e8f0,
        );

        for (l, t, w, h) in [
            (action_left, action_top, action_w, action_h),
            (left_left, left_top, left_w, left_h),
            (right_left, right_top, right_w, right_h),
            (controls_left, controls_top, controls_w, controls_h),
            (sources_left, sources_top, sources_w, sources_h),
        ] {
            push_rect(&mut states, l + w * 0.5, t + h * 0.5, w, h, 0x0f1727);
            draw_border(&mut states, l, t, w, h, 0x334155);
        }

        draw_text_line(
            &mut states,
            action_left + 12.0 * ui_scale,
            action_top + 12.0 * ui_scale,
            &format!("Action: {}", prompt.identifier),
            action_w - 24.0 * ui_scale,
            2.0 * ui_scale,
            0xbfdbfe,
        );
        draw_text_line(
            &mut states,
            action_left + 12.0 * ui_scale,
            action_top + 36.0 * ui_scale,
            &format!("Input Type: {:?}", prompt.input_type),
            action_w - 24.0 * ui_scale,
            2.0 * ui_scale,
            0x93c5fd,
        );

        let scope_label = if prompt.any_device_scope { "Any Device (*)" } else { "Exact Device" };
        let captured = prompt
            .suggestion
            .as_ref()
            .map(|path| path.with_device_scope(prompt.any_device_scope).to_string())
            .unwrap_or_else(|| "No input captured".to_string());
        draw_text_line(
            &mut states,
            left_left + 12.0 * ui_scale,
            left_top + 10.0 * ui_scale,
            "Device Scope",
            left_w - 24.0 * ui_scale,
            2.0 * ui_scale,
            0xf8fafc,
        );
        draw_text_line(
            &mut states,
            left_left + 12.0 * ui_scale,
            left_top + 36.0 * ui_scale,
            scope_label,
            left_w - 24.0 * ui_scale,
            2.0 * ui_scale,
            0xfde68a,
        );
        draw_text_line(
            &mut states,
            right_left + 12.0 * ui_scale,
            right_top + 10.0 * ui_scale,
            "Captured Input",
            right_w - 24.0 * ui_scale,
            2.0 * ui_scale,
            0xf8fafc,
        );
        draw_text_line(
            &mut states,
            right_left + 12.0 * ui_scale,
            right_top + 36.0 * ui_scale,
            &captured,
            right_w - 24.0 * ui_scale,
            2.0 * ui_scale,
            0x86efac,
        );

        draw_text_line(
            &mut states,
            controls_left + 12.0 * ui_scale,
            controls_top + 10.0 * ui_scale,
            "Controls",
            controls_w - 24.0 * ui_scale,
            2.0 * ui_scale,
            0xf8fafc,
        );
        draw_text_line(
            &mut states,
            controls_left + 12.0 * ui_scale,
            controls_top + 36.0 * ui_scale,
            "Enter confirm | Backspace skip | Tab toggle scope | Esc exit",
            controls_w - 24.0 * ui_scale,
            2.0 * ui_scale,
            0xfde68a,
        );

        draw_text_line(
            &mut states,
            sources_left + 12.0 * ui_scale,
            sources_top + 10.0 * ui_scale,
            "Supported Inputs",
            sources_w - 24.0 * ui_scale,
            2.0 * ui_scale,
            0xf8fafc,
        );
        let mut y = sources_top + 36.0 * ui_scale;
        let max_y = sources_top + sources_h - 16.0 * ui_scale;
        let line_h = 16.0 * ui_scale;
        for line in friendly_supported_lines(prompt) {
            if y > max_y {
                break;
            }
            draw_text_line(
                &mut states,
                sources_left + 12.0 * ui_scale,
                y,
                &format!("- {line}"),
                sources_w - 24.0 * ui_scale,
                2.0 * ui_scale,
                0xcbd5e1,
            );
            y += line_h;
        }

        states
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

    fn write_instances(&mut self, render_states: &[RenderState]) {
        if render_states.is_empty() {
            return;
        }
        self.instance_staging.clear();
        self.instance_staging.extend(render_states.iter().map(|state| InstanceRaw {
            center: [state.x, state.y],
            size: [state.width, state.height],
            color: unpack_color(state.color),
        }));
        self.queue.write_buffer(
            &self.instance_buffer,
            0,
            bytemuck::cast_slice(&self.instance_staging),
        );
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
    states.push(RenderState { x: sx, y: sy, width: sw, height: sh, color });
}

fn draw_border(
    states: &mut Vec<RenderState>,
    left: f32,
    top: f32,
    width: f32,
    height: f32,
    color: u32,
) {
    let t = 2.0;
    push_rect(states, left + width * 0.5, top + t * 0.5, width, t, color);
    push_rect(states, left + width * 0.5, top + height - t * 0.5, width, t, color);
    push_rect(states, left + t * 0.5, top + height * 0.5, t, height, color);
    push_rect(states, left + width - t * 0.5, top + height * 0.5, t, height, color);
}

fn draw_text_line(
    states: &mut Vec<RenderState>,
    x: f32,
    y: f32,
    text: &str,
    max_width: f32,
    scale: f32,
    color: u32,
) {
    let snapped_scale = scale.max(1.0).round();
    let text = text.to_ascii_uppercase();
    let char_w = 6.0 * snapped_scale;
    let max_chars = (max_width / char_w).floor().max(1.0) as usize;
    let total_chars = text.chars().count();
    let clipped = if total_chars > max_chars {
        let take = max_chars.saturating_sub(3);
        let mut out: String = text.chars().take(take).collect();
        out.push_str("...");
        out
    } else {
        text
    };
    draw_text(states, x.round(), y.round(), &clipped, snapped_scale, color);
}

fn draw_text(states: &mut Vec<RenderState>, x: f32, y: f32, text: &str, scale: f32, color: u32) {
    let mut cursor_x = x;
    for ch in text.chars() {
        if let Some(rows) = glyph_rows(ch) {
            for (row, pattern) in rows.iter().enumerate() {
                for col in 0..5 {
                    if (pattern >> (4 - col)) & 1 == 1 {
                        let px = cursor_x + col as f32 * scale;
                        let py = y + row as f32 * scale;
                        push_rect(states, px + scale * 0.5, py + scale * 0.5, scale, scale, color);
                    }
                }
            }
        }
        cursor_x += 6.0 * scale;
    }
}

fn glyph_rows(ch: char) -> Option<[u8; 7]> {
    match ch {
        'A' => Some([0b01110, 0b10001, 0b10001, 0b11111, 0b10001, 0b10001, 0b10001]),
        'B' => Some([0b11110, 0b10001, 0b10001, 0b11110, 0b10001, 0b10001, 0b11110]),
        'C' => Some([0b01110, 0b10001, 0b10000, 0b10000, 0b10000, 0b10001, 0b01110]),
        'D' => Some([0b11110, 0b10001, 0b10001, 0b10001, 0b10001, 0b10001, 0b11110]),
        'E' => Some([0b11111, 0b10000, 0b10000, 0b11110, 0b10000, 0b10000, 0b11111]),
        'F' => Some([0b11111, 0b10000, 0b10000, 0b11110, 0b10000, 0b10000, 0b10000]),
        'G' => Some([0b01110, 0b10001, 0b10000, 0b10111, 0b10001, 0b10001, 0b01110]),
        'H' => Some([0b10001, 0b10001, 0b10001, 0b11111, 0b10001, 0b10001, 0b10001]),
        'I' => Some([0b01110, 0b00100, 0b00100, 0b00100, 0b00100, 0b00100, 0b01110]),
        'J' => Some([0b00001, 0b00001, 0b00001, 0b00001, 0b10001, 0b10001, 0b01110]),
        'K' => Some([0b10001, 0b10010, 0b10100, 0b11000, 0b10100, 0b10010, 0b10001]),
        'L' => Some([0b10000, 0b10000, 0b10000, 0b10000, 0b10000, 0b10000, 0b11111]),
        'M' => Some([0b10001, 0b11011, 0b10101, 0b10101, 0b10001, 0b10001, 0b10001]),
        'N' => Some([0b10001, 0b10001, 0b11001, 0b10101, 0b10011, 0b10001, 0b10001]),
        'O' => Some([0b01110, 0b10001, 0b10001, 0b10001, 0b10001, 0b10001, 0b01110]),
        'P' => Some([0b11110, 0b10001, 0b10001, 0b11110, 0b10000, 0b10000, 0b10000]),
        'Q' => Some([0b01110, 0b10001, 0b10001, 0b10001, 0b10101, 0b10010, 0b01101]),
        'R' => Some([0b11110, 0b10001, 0b10001, 0b11110, 0b10100, 0b10010, 0b10001]),
        'S' => Some([0b01111, 0b10000, 0b10000, 0b01110, 0b00001, 0b00001, 0b11110]),
        'T' => Some([0b11111, 0b00100, 0b00100, 0b00100, 0b00100, 0b00100, 0b00100]),
        'U' => Some([0b10001, 0b10001, 0b10001, 0b10001, 0b10001, 0b10001, 0b01110]),
        'V' => Some([0b10001, 0b10001, 0b10001, 0b10001, 0b10001, 0b01010, 0b00100]),
        'W' => Some([0b10001, 0b10001, 0b10001, 0b10101, 0b10101, 0b10101, 0b01010]),
        'X' => Some([0b10001, 0b10001, 0b01010, 0b00100, 0b01010, 0b10001, 0b10001]),
        'Y' => Some([0b10001, 0b10001, 0b01010, 0b00100, 0b00100, 0b00100, 0b00100]),
        'Z' => Some([0b11111, 0b00001, 0b00010, 0b00100, 0b01000, 0b10000, 0b11111]),
        '0' => Some([0b01110, 0b10001, 0b10011, 0b10101, 0b11001, 0b10001, 0b01110]),
        '1' => Some([0b00100, 0b01100, 0b00100, 0b00100, 0b00100, 0b00100, 0b01110]),
        '2' => Some([0b01110, 0b10001, 0b00001, 0b00010, 0b00100, 0b01000, 0b11111]),
        '3' => Some([0b11110, 0b00001, 0b00001, 0b01110, 0b00001, 0b00001, 0b11110]),
        '4' => Some([0b00010, 0b00110, 0b01010, 0b10010, 0b11111, 0b00010, 0b00010]),
        '5' => Some([0b11111, 0b10000, 0b10000, 0b11110, 0b00001, 0b00001, 0b11110]),
        '6' => Some([0b01110, 0b10000, 0b10000, 0b11110, 0b10001, 0b10001, 0b01110]),
        '7' => Some([0b11111, 0b00001, 0b00010, 0b00100, 0b01000, 0b01000, 0b01000]),
        '8' => Some([0b01110, 0b10001, 0b10001, 0b01110, 0b10001, 0b10001, 0b01110]),
        '9' => Some([0b01110, 0b10001, 0b10001, 0b01111, 0b00001, 0b00001, 0b01110]),
        ' ' => Some([0, 0, 0, 0, 0, 0, 0]),
        ':' => Some([0, 0b00100, 0, 0, 0b00100, 0, 0]),
        '-' => Some([0, 0, 0, 0b11111, 0, 0, 0]),
        '_' => Some([0, 0, 0, 0, 0, 0, 0b11111]),
        '/' => Some([0b00001, 0b00010, 0b00100, 0b01000, 0b10000, 0, 0]),
        '*' => Some([0, 0b10101, 0b01110, 0b11111, 0b01110, 0b10101, 0]),
        '(' => Some([0b00010, 0b00100, 0b01000, 0b01000, 0b01000, 0b00100, 0b00010]),
        ')' => Some([0b01000, 0b00100, 0b00010, 0b00010, 0b00010, 0b00100, 0b01000]),
        '[' => Some([0b01110, 0b01000, 0b01000, 0b01000, 0b01000, 0b01000, 0b01110]),
        ']' => Some([0b01110, 0b00010, 0b00010, 0b00010, 0b00010, 0b00010, 0b01110]),
        '|' => Some([0b00100, 0b00100, 0b00100, 0b00100, 0b00100, 0b00100, 0b00100]),
        '.' => Some([0, 0, 0, 0, 0, 0b00100, 0]),
        '>' => Some([0b10000, 0b01000, 0b00100, 0b00010, 0b00100, 0b01000, 0b10000]),
        '<' => Some([0b00001, 0b00010, 0b00100, 0b01000, 0b00100, 0b00010, 0b00001]),
        ',' => Some([0, 0, 0, 0, 0b00100, 0b00100, 0b01000]),
        '=' => Some([0, 0b11111, 0, 0b11111, 0, 0, 0]),
        '?' => Some([0b01110, 0b10001, 0b00001, 0b00010, 0b00100, 0, 0b00100]),
        '!' => Some([0b00100, 0b00100, 0b00100, 0b00100, 0b00100, 0, 0b00100]),
        _ => None,
    }
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
    if prompt.joystick_scalar_fallback {
        lines.push("2D joysticks currently send scalar values (protocol limitation)".to_string());
    }
    lines.push("Use Tab to switch between exact device and wildcard (*)".to_string());
    lines
}
