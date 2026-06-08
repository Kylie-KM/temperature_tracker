use chrono::{TimeZone, Utc};
use eframe::egui;
use egui_plot::{Line, Plot, PlotPoints, PlotUi};
use rusqlite::{params, Connection};
use serde::Deserialize;
use std::path::PathBuf;
use std::sync::mpsc;
use std::thread;

// ==========================================
// 1. DATA STRUCTURES & API MODELS
// ==========================================

#[derive(Debug, Clone)]
struct TemperatureRecord {
    timestamp: i64, 
    temperature: f64,
}

#[derive(Deserialize, Debug)]
struct OpenMeteoResponse {
    utc_offset_seconds: i32,
    hourly: HourlyData,
}

#[derive(Deserialize, Debug)]
struct HourlyData {
    // Open-Meteo now returns absolute unix timestamps
    time: Vec<i64>, 
    temperature_2m: Vec<Option<f64>>, 
}

enum FetchMsg {
    Done(Result<i32, String>), // Now returns the utc_offset_seconds on success
}

// ==========================================
// 2. DATABASE LAYER
// ==========================================

struct DbManager {
    conn: Connection,
}

impl DbManager {
    fn init() -> Self {
        let mut path = PathBuf::from(".");
        path.push("weather_cache.db");
        let conn = Connection::open(path).expect("Failed to open SQLite database");
        
        conn.execute(
            "CREATE TABLE IF NOT EXISTS weather_data (
                location TEXT,
                timestamp INTEGER,
                temperature REAL,
                PRIMARY KEY (location, timestamp)
            )",
            [],
        ).expect("Failed to initialize data table");

        conn.execute(
            "CREATE TABLE IF NOT EXISTS app_state (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                location TEXT,
                lat TEXT,
                lon TEXT
            )",
            [],
        ).expect("Failed to initialize state table");

        conn.execute(
            "CREATE TABLE IF NOT EXISTS saved_locations (
                location TEXT PRIMARY KEY,
                lat TEXT,
                lon TEXT
            )",
            [],
        ).expect("Failed to initialize saved_locations table");

        // Safe schema migrations: Add utc_offset columns if they don't exist yet
        let _ = conn.execute("ALTER TABLE app_state ADD COLUMN utc_offset INTEGER DEFAULT 0", []);
        let _ = conn.execute("ALTER TABLE saved_locations ADD COLUMN utc_offset INTEGER DEFAULT 0", []);

        DbManager { conn }
    }

    fn load_state(&self) -> (String, String, String, i32) {
        let mut stmt = self.conn.prepare("SELECT location, lat, lon, utc_offset FROM app_state WHERE id = 1").unwrap();
        let mut rows = stmt.query([]).unwrap();
        
        if let Some(row) = rows.next().unwrap() {
            (
                row.get(0).unwrap_or_default(),
                row.get(1).unwrap_or_default(),
                row.get(2).unwrap_or_default(),
                row.get(3).unwrap_or(0)
            )
        } else {
            (String::new(), "53.5461".to_string(), "-113.4938".to_string(), 0)
        }
    }

    fn save_state(&self, loc: &str, lat: &str, lon: &str, offset: i32) {
        self.conn.execute(
            "INSERT OR REPLACE INTO app_state (id, location, lat, lon, utc_offset) VALUES (1, ?1, ?2, ?3, ?4)",
            params![loc, lat, lon, offset],
        ).unwrap();
    }

    fn save_location_profile(&self, loc: &str, lat: &str, lon: &str, offset: i32) {
        self.conn.execute(
            "INSERT OR REPLACE INTO saved_locations (location, lat, lon, utc_offset) VALUES (?1, ?2, ?3, ?4)",
            params![loc, lat, lon, offset],
        ).unwrap();
    }

    fn get_saved_locations(&self) -> Vec<(String, String, String, i32)> {
        let mut stmt = self.conn.prepare("SELECT location, lat, lon, utc_offset FROM saved_locations ORDER BY location ASC").unwrap();
        let rows = stmt.query_map([], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
        }).unwrap();

        rows.filter_map(|r| r.ok()).collect()
    }

    fn save_records(&self, location: &str, records: &[TemperatureRecord]) {
        let mut stmt = self.conn
            .prepare("INSERT OR REPLACE INTO weather_data (location, timestamp, temperature) VALUES (?1, ?2, ?3)")
            .unwrap();
        
        for record in records {
            let _ = stmt.execute(params![location, record.timestamp, record.temperature]);
        }
    }

    fn get_records(&self, location: &str, start_ts: i64, end_ts: i64) -> Vec<TemperatureRecord> {
        let mut stmt = self.conn
            .prepare("SELECT timestamp, temperature FROM weather_data WHERE location = ?1 AND timestamp >= ?2 AND timestamp <= ?3 ORDER BY timestamp ASC")
            .unwrap();
        
        let rows = stmt.query_map(params![location, start_ts, end_ts], |row| {
            Ok(TemperatureRecord {
                timestamp: row.get(0)?,
                temperature: row.get(1)?,
            })
        }).unwrap();

        rows.filter_map(|r| r.ok()).collect()
    }

    fn get_date_range(&self, location: &str) -> Option<(i64, i64)> {
        let mut stmt = self.conn
            .prepare("SELECT MIN(timestamp), MAX(timestamp) FROM weather_data WHERE location = ?1")
            .unwrap();
        
        let mut rows = stmt.query(params![location]).unwrap();
        
        if let Some(row) = rows.next().unwrap() {
            let min: Option<i64> = row.get(0).unwrap_or(None);
            let max: Option<i64> = row.get(1).unwrap_or(None);
            if let (Some(min_val), Some(max_val)) = (min, max) {
                return Some((min_val, max_val));
            }
        }
        None
    }
}

// ==========================================
// 3. NETWORKING LAYER
// ==========================================

fn fetch_open_meteo_url(db: &DbManager, location: &str, url: &str) -> Result<i32, String> {
    let response: OpenMeteoResponse = reqwest::blocking::get(url)
        .map_err(|e| e.to_string())?
        .json()
        .map_err(|e| e.to_string())?;

    let mut records = Vec::new();
    
    // API is now returning clean unix timestamps, so no complex parsing is needed here!
    for (&ts, temp_opt) in response.hourly.time.iter().zip(response.hourly.temperature_2m.iter()) {
        if let Some(temp) = temp_opt {
            records.push(TemperatureRecord {
                timestamp: ts,
                temperature: *temp,
            });
        }
    }

    db.save_records(location, &records);
    Ok(response.utc_offset_seconds)
}

fn fetch_and_cache_weather(db: &DbManager, location: &str, lat: f64, lon: f64) -> Result<i32, String> {
    let now = Utc::now();
    let (_, max_ts) = db.get_date_range(location).unwrap_or((0, 0));
    
    let gap_days = if max_ts == 0 { 999 } else { (now.timestamp() - max_ts) / 86400 };

    if gap_days > 90 {
        let start_ts = if max_ts == 0 { now.timestamp() - (365 * 86400) } else { max_ts };
        let start_date = Utc.timestamp_opt(start_ts, 0).unwrap().format("%Y-%m-%d");
        let end_date = (now - chrono::Duration::try_days(5).unwrap()).format("%Y-%m-%d");

        // Added &timeformat=unixtime&timezone=auto
        let archive_url = format!(
            "https://archive-api.open-meteo.com/v1/archive?latitude={}&longitude={}&start_date={}&end_date={}&hourly=temperature_2m&timeformat=unixtime&timezone=auto",
            lat, lon, start_date, end_date
        );
        
        let _ = fetch_open_meteo_url(db, location, &archive_url);
    }

    let recent_days = gap_days.clamp(1, 92);
    // Added &timeformat=unixtime&timezone=auto
    let forecast_url = format!(
        "https://api.open-meteo.com/v1/forecast?latitude={}&longitude={}&hourly=temperature_2m&past_days={}&forecast_days=1&timeformat=unixtime&timezone=auto",
        lat, lon, recent_days
    );
    
    let offset = fetch_open_meteo_url(db, location, &forecast_url)?;

    Ok(offset)
}

// ==========================================
// 4. GRAPHICAL USER INTERFACE
// ==========================================

#[derive(PartialEq, Clone, Copy)]
enum GraphViewMode {
    Hourly,
    DailyHigh,
    DailyLow,
    DailyMinMax,
}

struct WeatherApp {
    db: DbManager,
    current_location: String,
    lat_input: String,
    lon_input: String,
    current_utc_offset: i32,
    location_error: Option<String>,
    
    saved_location: String,
    saved_lat: String,
    saved_lon: String,
    saved_utc_offset: i32,

    saved_locations_list: Vec<(String, String, String, i32)>,

    view_mode: GraphViewMode,
    start_days_ago: i32,
    end_days_ago: i32,
    
    unlock_history: bool,
    cached_days_available: i32,

    fetch_receiver: Option<mpsc::Receiver<FetchMsg>>,
    is_fetching: bool,
}

impl WeatherApp {
    fn new() -> Self {
        let db = DbManager::init();
        let (loc, lat, lon, offset) = db.load_state();
        let saved_locations_list = db.get_saved_locations();
        let (min_ts, _) = db.get_date_range(&loc).unwrap_or((Utc::now().timestamp(), 0));
        let cached_days = (((Utc::now().timestamp() - min_ts) / 86400) as i32).max(7);
        
        let mut app = Self {
            db,
            current_location: loc.clone(),
            lat_input: lat.clone(),
            lon_input: lon.clone(),
            current_utc_offset: offset,
            location_error: None,
            saved_location: loc.clone(),
            saved_lat: lat.clone(),
            saved_lon: lon.clone(),
            saved_utc_offset: offset,
            saved_locations_list,
            view_mode: GraphViewMode::Hourly,
            start_days_ago: 7,
            end_days_ago: 0,
            unlock_history: false,
            cached_days_available: cached_days,
            fetch_receiver: None,
            is_fetching: false,
        };

        if !app.current_location.is_empty() {
            if let (Ok(lat_val), Ok(lon_val)) = (app.lat_input.parse::<f64>(), app.lon_input.parse::<f64>()) {
                app.start_fetch(app.current_location.clone(), lat_val, lon_val);
            }
        }

        app
    }

    fn start_fetch(&mut self, loc: String, lat: f64, lon: f64) {
        self.is_fetching = true;
        self.location_error = None;
        
        let (tx, rx) = mpsc::channel();
        self.fetch_receiver = Some(rx);

        thread::spawn(move || {
            let thread_db = DbManager::init();
            let result = fetch_and_cache_weather(&thread_db, &loc, lat, lon);
            
            if let Ok(offset) = result {
                thread_db.save_location_profile(&loc, &lat.to_string(), &lon.to_string(), offset);
            }
            
            let _ = tx.send(FetchMsg::Done(result));
        });
    }

    fn process_daily_metrics(&self, records: &[TemperatureRecord], get_high: bool, offset: i64) -> Vec<TemperatureRecord> {
        use std::collections::BTreeMap;
        let mut grouped: BTreeMap<String, Vec<f64>> = BTreeMap::new();
        
        for record in records {
            // Shift timestamp by offset to determine the local date
            let local_date = Utc.timestamp_opt(record.timestamp + offset, 0)
                .unwrap()
                .format("%Y-%m-%d")
                .to_string();
            grouped.entry(local_date).or_default().push(record.temperature);
        }

        let mut daily_records = Vec::new();
        for (date_str, temps) in grouped {
            if temps.is_empty() { continue; }
            
            let final_temp = if get_high {
                temps.into_iter().fold(f64::MIN, f64::max)
            } else {
                temps.into_iter().fold(f64::MAX, f64::min)
            };

            if let Ok(naive_date) = chrono::NaiveDate::parse_from_str(&date_str, "%Y-%m-%d") {
				if let Some(naive_datetime) = naive_date.and_hms_opt(12, 0, 0) {
					// explicitly cast the naive time to UTC before grabbing the timestamp
					let fake_utc_ts = naive_datetime.and_utc().timestamp(); 
					let absolute_ts = fake_utc_ts - offset; // Reverse the offset so it graphs perfectly at noon locally
        
					daily_records.push(TemperatureRecord {
					timestamp: absolute_ts,
					temperature: final_temp,
					});
				}
			}
        }
        daily_records
    }

    fn render_plot(&self, ui: &mut egui::Ui, datasets: Vec<(Vec<TemperatureRecord>, egui::Color32)>) {
        if datasets.iter().all(|(records, _)| records.is_empty()) {
            ui.label("No data available within this timeframe. Update location or adjust ranges.");
            return;
        }

        let mut min_temp = f64::MAX;
        let mut max_temp = f64::MIN;
        let mut min_time = i64::MAX;
        let mut max_time = i64::MIN;

        let mut lines = Vec::new();

        for (records, color) in &datasets {
            if records.is_empty() { continue; }
            
            let points: PlotPoints = records
                .iter()
                .map(|r| {
                    min_temp = min_temp.min(r.temperature);
                    max_temp = max_temp.max(r.temperature);
                    min_time = min_time.min(r.timestamp);
                    max_time = max_time.max(r.timestamp);
                    [r.timestamp as f64, r.temperature]
                })
                .collect();
            
            lines.push(Line::new(points).color(*color));
        }

        if min_time == max_time {
            min_time -= 3600;
            max_time += 3600;
        }

        let current_offset = self.current_utc_offset as i64;

        egui::Frame::canvas(ui.style())
            .inner_margin(egui::Margin::same(16.0)) 
            .show(ui, |ui| {
                let plot_response = Plot::new("weather_plot")
                    .height(ui.available_height() - 5.0) 
                    .allow_zoom(false)
                    .allow_drag(false)
                    .allow_scroll(false)
                    .include_y(min_temp - 5.0)
                    .include_y(max_temp + 5.0)
                    .include_x(min_time as f64)
                    .include_x(max_time as f64)
                    .x_axis_formatter(move |tick, _, _| {
                        // Apply location's UTC offset directly instead of system's local timezone
                        let target_local_time = Utc.timestamp_opt(tick.value as i64 + current_offset, 0).unwrap();
                        target_local_time.format("%m/%d %H:%M").to_string()
                    })
                    .y_axis_formatter(|tick, _, _| {
                        // Fixed: Formats ticks as whole numbers to eliminate decimal clutter
                        format!("{:.0}°C", tick.value)
                    })
                    .label_formatter(|_, _| String::new())
                    .show(ui, |plot_ui: &mut PlotUi| {
                        for line in lines {
                            plot_ui.line(line);
                        }
                        plot_ui.pointer_coordinate()
                    });

                if plot_response.response.hovered() {
                    if let Some(plot_pos) = plot_response.inner {
                        
                        let mut closest_x_diff = f64::MAX;
                        let mut target_timestamp = 0;

                        for (records, _) in &datasets {
                            for record in records {
                                let diff = (record.timestamp as f64 - plot_pos.x).abs();
                                if diff < closest_x_diff {
                                    closest_x_diff = diff;
                                    target_timestamp = record.timestamp;
                                }
                            }
                        }

                        if closest_x_diff < (3.0 * 3600.0) {
                            let mut best_record = None;
                            let mut closest_y_diff = f64::MAX;

                            for (records, _) in &datasets {
                                for record in records {
                                    if record.timestamp == target_timestamp {
                                        let y_diff = (record.temperature - plot_pos.y).abs();
                                        if y_diff < closest_y_diff {
                                            closest_y_diff = y_diff;
                                            best_record = Some(record);
                                        }
                                    }
                                }
                            }

                            if let Some(record) = best_record {
                                egui::show_tooltip_at_pointer(ui.ctx(), egui::Id::new("custom_plot_tooltip"), |ui| {
                                    // Apply the location's UTC offset for tooltip readouts as well
                                    let target_local_time = Utc.timestamp_opt(record.timestamp + current_offset, 0).unwrap();
                                    ui.label(format!("Time: {}", target_local_time.format("%Y-%m-%d %H:%M")));
                                    ui.label(format!("Temp: {:.1}°C", record.temperature));
                                });
                            }
                        }
                    }
                }
            });
    }
}

impl eframe::App for WeatherApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        
        if let Some(rx) = &self.fetch_receiver {
            if let Ok(FetchMsg::Done(res)) = rx.try_recv() {
                self.is_fetching = false;
                self.fetch_receiver = None;
                
                match res {
                    Ok(offset) => {
                        self.current_utc_offset = offset;
                        let (min_ts, _) = self.db.get_date_range(&self.current_location).unwrap_or((Utc::now().timestamp(), 0));
                        self.cached_days_available = (((Utc::now().timestamp() - min_ts) / 86400) as i32).max(7);
                        self.saved_locations_list = self.db.get_saved_locations();
                    },
                    Err(err) => self.location_error = Some(format!("Network/API Fault: {}", err)),
                }
            } else {
                ctx.request_repaint();
            }
        }

        if self.current_location != self.saved_location || 
           self.lat_input != self.saved_lat || 
           self.lon_input != self.saved_lon ||
           self.current_utc_offset != self.saved_utc_offset {
            
            self.db.save_state(&self.current_location, &self.lat_input, &self.lon_input, self.current_utc_offset);
            
            self.saved_location = self.current_location.clone();
            self.saved_lat = self.lat_input.clone();
            self.saved_lon = self.lon_input.clone();
            self.saved_utc_offset = self.current_utc_offset;

            let (min_ts, _) = self.db.get_date_range(&self.current_location).unwrap_or((Utc::now().timestamp(), 0));
            self.cached_days_available = (((Utc::now().timestamp() - min_ts) / 86400) as i32).max(7);
            
            if !self.unlock_history {
                self.start_days_ago = self.start_days_ago.min(7);
                self.end_days_ago = self.end_days_ago.min(7);
            }
        }

        egui::CentralPanel::default().show(ctx, |ui| {
            ui.horizontal(|ui| {
                ui.heading("Temperature Tracker");
                if self.is_fetching {
                    ui.add_space(10.0);
                    ui.colored_label(egui::Color32::YELLOW, "⚙ Syncing API data in background...");
                }
            });
            ui.add_space(8.0);
            
            egui::Ui::vertical(ui, |ui| {
                ctx.memory_mut(|mem| {
                    if mem.data.get_temp::<usize>(egui::Id::new("current_tab")).is_none() {
                        mem.data.insert_temp(egui::Id::new("current_tab"), 0usize);
                    }
                });

                let mut current_tab = ctx.memory(|mem| mem.data.get_temp::<usize>(egui::Id::new("current_tab")).unwrap_or(0));

                ui.horizontal(|ui| {
                    ui.selectable_value(&mut current_tab, 0, "72-Hour Overview");
                    ui.selectable_value(&mut current_tab, 1, "Custom Analytics & Configuration");
                });
                
                ctx.memory_mut(|mem| mem.data.insert_temp(egui::Id::new("current_tab"), current_tab));

                ui.separator();
                ui.add_space(10.0);

                if current_tab == 0 {
                    if self.current_location.is_empty() {
                        ui.colored_label(egui::Color32::LIGHT_RED, "⚠️ Location not set. Please check \"Custom Analytics & Configuration\" tab.");
                    } else {
                        ui.label(format!("Showing past 72 hours for: {}", self.current_location));
                        let end_time = Utc::now().timestamp();
                        let start_time = end_time - (72 * 60 * 60);
                        
                        let records = self.db.get_records(&self.current_location, start_time, end_time);
                        self.render_plot(ui, vec![(records, egui::Color32::RED)]);
                    }
                } else {
                    ui.label("Configuration");

                    if !self.saved_locations_list.is_empty() {
                        ui.horizontal(|ui| {
                            ui.label("Saved Locations:");
                            
                            egui::ComboBox::from_id_source("saved_locations_dropdown")
                                .selected_text("Load a Saved Location...") 
                                .show_ui(ui, |ui| {
                                    for (loc_name, lat_str, lon_str, offset_val) in &self.saved_locations_list {
                                        if ui.selectable_label(false, loc_name).clicked() {
                                            self.current_location = loc_name.clone();
                                            self.lat_input = lat_str.clone();
                                            self.lon_input = lon_str.clone();
                                            self.current_utc_offset = *offset_val;
                                        }
                                    }
                                });
                        });
                    }
                    
                    ui.horizontal(|ui| {
                        ui.label("Location Identifier:");
                        ui.text_edit_singleline(&mut self.current_location);
                    });

                    ui.horizontal(|ui| {
                        ui.label("Latitude:");
                        ui.text_edit_singleline(&mut self.lat_input);
                        ui.label("Longitude:");
                        ui.text_edit_singleline(&mut self.lon_input);
                    });

                    ui.add_enabled_ui(!self.is_fetching, |ui| {
                        if ui.button("Fetch and Sync Data").clicked() {
                            if let (Ok(lat), Ok(lon)) = (self.lat_input.parse::<f64>(), self.lon_input.parse::<f64>()) {
                                if self.current_location.trim().is_empty() {
                                    self.location_error = Some("Please define a location label name.".to_string());
                                } else {
                                    self.start_fetch(self.current_location.clone(), lat, lon);
                                }
                            } else {
                                self.location_error = Some("Invalid float coordinates configuration format.".to_string());
                            }
                        }
                    });

                    if let Some(ref err_msg) = self.location_error {
                        ui.colored_label(egui::Color32::LIGHT_RED, err_msg);
                    }

                    ui.separator();
                    ui.label("Graph Display Configurations");

                    ui.horizontal(|ui| {
                        ui.selectable_value(&mut self.view_mode, GraphViewMode::Hourly, "Hourly Resolution");
                        ui.selectable_value(&mut self.view_mode, GraphViewMode::DailyHigh, "Daily Highs");
                        ui.selectable_value(&mut self.view_mode, GraphViewMode::DailyLow, "Daily Lows");
                        ui.selectable_value(&mut self.view_mode, GraphViewMode::DailyMinMax, "Daily Min/Max");
                    });

                    ui.horizontal(|ui| {
                        let slider_max = if self.unlock_history { self.cached_days_available } else { 7 };
                        
                        ui.label("Start Frame (Days Ago):");
                        ui.add(egui::Slider::new(&mut self.start_days_ago, 0..=slider_max));
                        ui.label("End Frame (Days Ago):");
                        ui.add(egui::Slider::new(&mut self.end_days_ago, 0..=slider_max));
                    });

                    ui.checkbox(
                        &mut self.unlock_history, 
                        format!("Unlock Start/End Frame Sliders ({} days available locally)", self.cached_days_available)
                    );

                    ui.add_space(10.0);
                    ui.heading("");

                    if !self.current_location.is_empty() {
                        let now_ts = Utc::now().timestamp();
                        let start_ts = now_ts - (self.start_days_ago as i64 * 24 * 60 * 60);
                        let end_ts = now_ts - (self.end_days_ago as i64 * 24 * 60 * 60);

                        let raw_records = self.db.get_records(&self.current_location, start_ts, end_ts);
                        let offset = self.current_utc_offset as i64;
                        
                        let display_datasets = match self.view_mode {
                            GraphViewMode::Hourly => {
                                vec![(raw_records, egui::Color32::RED)]
                            },
                            GraphViewMode::DailyHigh => {
                                vec![(self.process_daily_metrics(&raw_records, true, offset), egui::Color32::RED)]
                            },
                            GraphViewMode::DailyLow => {
                                vec![(self.process_daily_metrics(&raw_records, false, offset), egui::Color32::LIGHT_BLUE)]
                            },
                            GraphViewMode::DailyMinMax => {
                                vec![
                                    (self.process_daily_metrics(&raw_records, true, offset), egui::Color32::RED),
                                    (self.process_daily_metrics(&raw_records, false, offset), egui::Color32::LIGHT_BLUE),
                                ]
                            },
                        };

                        self.render_plot(ui, display_datasets);
                    } else {
                        ui.label("Please initialize location configuration above to map custom frames.");
                    }
                }
            });
        });
    }
}

fn main() -> eframe::Result<()> {
    let native_options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default().with_inner_size([900.0, 650.0]),
        ..Default::default()
    };
    
    eframe::run_native(
        "Temperature Tracker",
        native_options,
        Box::new(|_cc| Box::new(WeatherApp::new())),
    )
}