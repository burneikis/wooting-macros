#[cfg(not(debug_assertions))]
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::{thread, time};

use anyhow::{bail, Error, Result};
#[cfg(not(debug_assertions))]
use dirs;
use halfbrown::HashMap;
use itertools::Itertools;
use log::*;
use rayon::prelude::*;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::sync::RwLock;
use tokio::task;

use config::{ApplicationConfig, ConfigFile};

// This has to be imported for release build
#[allow(unused_imports)]
use crate::config::CONFIG_DIR;
use crate::hid_table::*;
//Plugin imports
use crate::plugin::delay;
#[allow(unused_imports)]
use crate::plugin::discord;
use crate::plugin::key_press;
use crate::plugin::mouse;
#[allow(unused_imports)]
use crate::plugin::obs;
use crate::plugin::phillips_hue;
use crate::plugin::system_event;

pub mod config;
mod hid_table;
pub mod plugin;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq)]
/// Type of a macro. Currently only Single is implemented. Others have been postponed for now.
///
/// ! **UNIMPLEMENTED** - Only the `Single` macro type is implemented for now. Feel free to contribute ideas.
pub enum MacroType {
    Single,
    // Single macro fire
    Toggle,
    // press to start, press to finish cycle and terminate
    OnHold, // while held Execute macro (repeats)
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(tag = "type")]
/// This enum is the registry for all actions that can be executed.
pub enum ActionEventType {
    KeyPressEventAction {
        data: key_press::KeyPress,
    },
    SystemEventAction {
        data: system_event::SystemAction,
    },
    //Paste, Run commandline program (terminal run? standard user?), audio, open file-manager, workspace switch left, right,
    //IDEA: System event - notification
    PhillipsHueEventAction {
        data: phillips_hue::PhillipsHueStatus,
    },
    //IDEA: Phillips hue notification
    OBSEventAction {},

    DiscordEventAction {},
    //IDEA: IKEADesk
    MouseEventAction {
        data: mouse::MouseAction,
    },
    //IDEA: Sound effects? Soundboards?
    //IDEA: Sending a message through online webapi (twitch)
    DelayEventAction {
        data: delay::Delay,
    },
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq)]
#[serde(tag = "type")]
/// This enum is the registry for all incoming actions that can be analyzed for macro execution.
///
/// ! **UNIMPLEMENTED** - Allow while other keys has not been implemented yet. This is WIP already.
pub enum TriggerEventType {
    KeyPressEvent {
        data: Vec<u32>,
        allow_while_other_keys: bool,
    },
    MouseEvent {
        data: mouse::MouseButton,
    },
    //IDEA: computer time (have timezone support?)
    //IDEA: computer temperature?
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
/// This is a macro struct. Includes all information a macro needs to run.
pub struct Macro {
    pub name: String,
    pub icon: String,
    pub sequence: Vec<ActionEventType>,
    pub macro_type: MacroType,
    pub trigger: TriggerEventType,
    pub active: bool,
}

impl Macro {
    /// This function is used to execute a macro. It is called by the macro checker.
    /// It spawns async tasks to execute said events specifically.
    /// Make sure to expand this if you implement new action types.
    async fn execute(&self, send_channel: UnboundedSender<rdev::EventType>) -> Result<()> {
        for action in &self.sequence {
            match action {
                ActionEventType::KeyPressEventAction { data } => match data.keytype {
                    key_press::KeyType::Down => {
                        // One key press down
                        send_channel
                            .send(rdev::EventType::KeyPress(SCANCODE_TO_RDEV[&data.keypress]))?;
                    }
                    key_press::KeyType::Up => {
                        // One key lift up
                        send_channel.send(rdev::EventType::KeyRelease(
                            SCANCODE_TO_RDEV[&data.keypress],
                        ))?;
                    }
                    key_press::KeyType::DownUp => {
                        // Key press
                        send_channel
                            .send(rdev::EventType::KeyPress(SCANCODE_TO_RDEV[&data.keypress]))?;

                        // Wait the set delay by user
                        tokio::time::sleep(time::Duration::from_millis(data.press_duration)).await;

                        // Lift the key
                        send_channel.send(rdev::EventType::KeyRelease(
                            SCANCODE_TO_RDEV[&data.keypress],
                        ))?;
                    }
                },
                ActionEventType::PhillipsHueEventAction { .. } => {}
                ActionEventType::OBSEventAction { .. } => {}
                ActionEventType::DiscordEventAction { .. } => {}
                ActionEventType::DelayEventAction { data } => {
                    tokio::time::sleep(time::Duration::from_millis(*data)).await;
                }

                ActionEventType::SystemEventAction { data } => {
                    let action_copy = data.clone();
                    let channel_copy = send_channel.clone();
                    task::spawn(async move { action_copy.execute(channel_copy).await });
                }
                ActionEventType::MouseEventAction { data } => {
                    let action_copy = data.clone();
                    let channel_copy = send_channel.clone();
                    task::spawn(async move { action_copy.execute(channel_copy).await });
                }
            }
        }
        Ok(())
    }
}

/// Collections are groups of macros.
type Collections = Vec<Collection>;

/// Hashmap to check the first trigger key of each macro.
type MacroTriggerLookup = HashMap<u32, Vec<Macro>>;

/// State of the application in RAM (RWlock).
#[derive(Debug)]
pub struct MacroBackend {
    pub data: Arc<RwLock<MacroData>>,
    pub config: Arc<RwLock<ApplicationConfig>>,
    pub triggers: Arc<RwLock<MacroTriggerLookup>>,
    pub is_listening: Arc<AtomicBool>,
    pub onhold_states: Arc<RwLock<HashMap<String, bool>>>,
    pub onhold_handles: Arc<RwLock<HashMap<String, tokio::task::JoinHandle<()>>>>,
}

///MacroData is the main data structure that contains all macro data.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct MacroData {
    pub data: Collections,
}

impl Default for MacroData {
    fn default() -> Self {
        MacroData {
            data: vec![Collection {
                name: "Collection 1".to_string(),
                icon: ":smile:".to_string(),
                macros: vec![],
                active: true,
            }],
        }
    }
}

impl MacroData {
    /// Extracts the first trigger data from the macros.
    pub fn extract_triggers(&self) -> Result<MacroTriggerLookup> {
        let mut output_hashmap = MacroTriggerLookup::new();

        for collections in &self.data {
            if collections.active {
                for macros in &collections.macros {
                    if macros.active {
                        match &macros.trigger {
                            TriggerEventType::KeyPressEvent { data, .. } => {
                                //TODO: optimize using references
                                match data.len() {
                                    0 => {
                                        bail!("a trigger key can't be zero, aborting trigger generation: {:#?}", data);
                                    }
                                    1 => {
                                        let first_data = match data.first() {
                                            Some(data) => *data,
                                            None => {
                                                return Err(Error::msg(
                                                    "Error getting first element in macro trigger",
                                                ));
                                            }
                                        };
                                        output_hashmap
                                            .entry(first_data)
                                            .or_default()
                                            .push(macros.clone())
                                    }
                                    _ => data[..data.len() - 1].iter().for_each(|x| {
                                        output_hashmap.entry(*x).or_default().push(macros.clone());
                                    }),
                                }
                            }
                            TriggerEventType::MouseEvent { data } => {
                                let data: u32 = data.into();

                                match output_hashmap.get_mut(&data) {
                                    Some(value) => value.push(macros.clone()),
                                    None => {
                                        output_hashmap.insert_nocheck(data, vec![macros.clone()])
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        Ok(output_hashmap)
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
/// Collection struct that defines what a group of macros looks like and what properties it carries
pub struct Collection {
    pub name: String,
    pub icon: String,
    pub macros: Vec<Macro>,
    pub active: bool,
}

/// Helper function to generate macro ID from collection and macro indices
fn generate_macro_id(collection_index: usize, macro_index: usize) -> String {
    format!("{}_{}", collection_index, macro_index)
}

/// Helper function to find collection and macro indices for a given macro
fn find_macro_indices(macro_data: &MacroData, target_macro: &Macro) -> Option<(usize, usize)> {
    for (collection_index, collection) in macro_data.data.iter().enumerate() {
        for (macro_index, macro_item) in collection.macros.iter().enumerate() {
            // Compare by name and trigger (since macros might be cloned)
            if macro_item.name == target_macro.name && macro_item.trigger == target_macro.trigger {
                return Some((collection_index, macro_index));
            }
        }
    }
    None
}

/// Executes an onhold macro - starts when triggered and stops when key is released.
async fn execute_macro_onhold(
    macro_id: String,
    macros: Macro,
    channel: UnboundedSender<rdev::EventType>,
    onhold_states: Arc<RwLock<HashMap<String, bool>>>,
    onhold_handles: Arc<RwLock<HashMap<String, tokio::task::JoinHandle<()>>>>,
) {
    let mut onhold_states_lock = onhold_states.write().await;
    let mut onhold_handles_lock = onhold_handles.write().await;
    
    // Always start the onhold macro (unlike toggle which checks state)
    info!("STARTING ONHOLD MACRO: {:#?}", macros.name);
    
    // Stop any existing instance of this macro
    if let Some(handle) = onhold_handles_lock.remove(&macro_id) {
        handle.abort();
    }
    
    onhold_states_lock.insert(macro_id.clone(), true);
    
    let macro_clone = macros.clone();
    let channel_clone = channel.clone();
    let onhold_states_clone = onhold_states.clone();
    let macro_id_clone = macro_id.clone();
    
    let handle = task::spawn(async move {
        loop {
            // Check if we should stop (key released)
            let should_stop = {
                let onhold_states_read = onhold_states_clone.read().await;
                !onhold_states_read.get(&macro_id_clone).unwrap_or(&false)
            };
            
            if should_stop {
                break;
            }
            
            // Execute the macro sequence once
            if let Err(error) = macro_clone.execute(channel_clone.clone()).await {
                error!("error executing onhold macro: {}", error);
                break;
            }
            
            // Add a small delay before repeating
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        }
    });
    
    onhold_handles_lock.insert(macro_id, handle);
}

/// Stops an onhold macro
async fn stop_onhold_macro(
    macro_id: String,
    onhold_states: Arc<RwLock<HashMap<String, bool>>>,
    onhold_handles: Arc<RwLock<HashMap<String, tokio::task::JoinHandle<()>>>>,
) {
    let mut onhold_states_lock = onhold_states.write().await;
    let mut onhold_handles_lock = onhold_handles.write().await;
    
    info!("STOPPING ONHOLD MACRO: {}", macro_id);
    
    onhold_states_lock.insert(macro_id.clone(), false);
    if let Some(handle) = onhold_handles_lock.remove(&macro_id) {
        handle.abort();
    }
}

/// Executes a given macro (according to its type).
async fn execute_macro(
    macros: Macro, 
    channel: UnboundedSender<rdev::EventType>, 
    collection_index: usize, 
    macro_index: usize,
    onhold_states: Arc<RwLock<HashMap<String, bool>>>,
    onhold_handles: Arc<RwLock<HashMap<String, tokio::task::JoinHandle<()>>>>,
) {
    match macros.macro_type {
        MacroType::Single => {
            info!("\nEXECUTING A SINGLE MACRO: {:#?}", macros.name);

            let cloned_channel = channel;

            task::spawn(async move {
                if let Err(error) = macros.execute(cloned_channel).await {
                    error!("error executing macro: {}", error);
                }
            });
        }
        MacroType::Toggle => {
            //Postponed
            //execute_macro_toggle(&macros).await;
        }
        MacroType::OnHold => {
            let macro_id = generate_macro_id(collection_index, macro_index);
            execute_macro_onhold(macro_id, macros, channel, onhold_states, onhold_handles).await;
        }
    }
}

/// Receives and executes a macro based on the trigger event.
/// Puts a mandatory 0-20 ms delay between each macro execution (depending on the platform).
fn keypress_executor_sender(mut rchan_execute: UnboundedReceiver<rdev::EventType>) {
    loop {
        let received_event = match &rchan_execute.blocking_recv() {
            Some(event) => *event,
            None => {
                error!("Failed to receive an event!");
                continue;
            }
        };
        plugin::util::direct_send_event(&received_event)
            .unwrap_or_else(|err| error!("Error directly sending an event to keyboard: {}", err));

        //Every OS requires a delay so the OS can catch up.
        thread::sleep(time::Duration::from_millis(delay::STANDARD_KEYPRESS_DELAY));
    }
}

/// A more efficient way using hashtable to check whether the trigger keys match the macro.
///
/// `pressed_events` - the keys pressed in HID format (use the conversion HID hashtable to get the number).
///
/// `trigger_overview` - Macros that need to be checked. Should be picked by matching the hashtable of triggers, and those should be checked here.
///
/// `channel_sender` - a copy of the channel sender to use later when executing various macros.
fn check_macro_execution_efficiently(
    pressed_events: Vec<u32>,
    trigger_overview: Vec<Macro>,
    channel_sender: UnboundedSender<rdev::EventType>,
    onhold_states: Arc<RwLock<HashMap<String, bool>>>,
    onhold_handles: Arc<RwLock<HashMap<String, tokio::task::JoinHandle<()>>>>,
    macro_data: Arc<RwLock<MacroData>>,
) -> bool {
    let trigger_overview_print = trigger_overview.clone();

    trace!("Got data: {:?}", trigger_overview_print);
    trace!("Got keys: {:?}", pressed_events);

    let mut output = false;
    for macros in &trigger_overview {
        match &macros.trigger {
            TriggerEventType::KeyPressEvent { data, .. } => {
                match data.len() {
                    1 => {
                        if pressed_events == *data {
                            debug!("MATCHED MACRO singlekey: {:#?}", pressed_events);

                            let channel_clone_execute = channel_sender.clone();
                            let macro_clone_execute = macros.clone();
                            let onhold_states_clone = onhold_states.clone();
                            let onhold_handles_clone = onhold_handles.clone();
                            let macro_data_clone = macro_data.clone();

                            // We don't need this here as there can't be a single key that's a modifier
                            // plugin::util::lift_keys(data, &channel_clone_execute);

                            task::spawn(async move {
                                let data_read = macro_data_clone.read().await;
                                if let Some((collection_index, macro_index)) = find_macro_indices(&data_read, &macro_clone_execute) {
                                    execute_macro(macro_clone_execute, channel_clone_execute, collection_index, macro_index, onhold_states_clone, onhold_handles_clone).await;
                                }
                            });
                            output = true;
                        }
                    }
                    2..=4 => {
                        // This check makes sure the modifier keys (up to 3 keys in each trigger) can be of any order, and ensures the last key must match to the proper one.
                        if data[..(data.len() - 1)]
                            .iter()
                            .all(|x| pressed_events[..(pressed_events.len() - 1)].contains(x))
                            && pressed_events[pressed_events.len() - 1] == data[data.len() - 1]
                        {
                            debug!("MATCHED MACRO multikey: {:#?}", pressed_events);

                            let channel_clone_execute = channel_sender.clone();
                            let macro_clone_execute = macros.clone();
                            let onhold_states_clone = onhold_states.clone();
                            let onhold_handles_clone = onhold_handles.clone();
                            let macro_data_clone = macro_data.clone();

                            // This releases any trigger keys that have been held to make macros more reliable when used with modifier hotkeys.
                            plugin::util::lift_keys(data, &channel_clone_execute)
                                .unwrap_or_else(|err| error!("Error lifting keys: {}", err));

                            task::spawn(async move {
                                let data_read = macro_data_clone.read().await;
                                if let Some((collection_index, macro_index)) = find_macro_indices(&data_read, &macro_clone_execute) {
                                    execute_macro(macro_clone_execute, channel_clone_execute, collection_index, macro_index, onhold_states_clone, onhold_handles_clone).await;
                                }
                            });
                            output = true;
                        }
                    }
                    _ => (),
                }
            }
            TriggerEventType::MouseEvent { data } => {
                let event_to_check: Vec<u32> = vec![data.into()];

                trace!(
                    "CheckMacroExec: Converted mouse buttons to vec<u32>\n {:#?}",
                    event_to_check
                );

                if event_to_check == pressed_events {
                    let channel_clone = channel_sender.clone();
                    let macro_clone = macros.clone();
                    let onhold_states_clone = onhold_states.clone();
                    let onhold_handles_clone = onhold_handles.clone();
                    let macro_data_clone = macro_data.clone();

                    task::spawn(async move {
                        let data_read = macro_data_clone.read().await;
                        if let Some((collection_index, macro_index)) = find_macro_indices(&data_read, &macro_clone) {
                            execute_macro(macro_clone, channel_clone, collection_index, macro_index, onhold_states_clone, onhold_handles_clone).await;
                        }
                    });
                    output = true;
                }
            }
        }
    }

    output
}

#[derive(Debug, Clone, Default)]
struct KeysPressed(Arc<RwLock<Vec<rdev::Key>>>);

impl MacroBackend {
    /// Helper function to parse macro_id back to collection and macro indices
    fn parse_macro_id(&self, macro_id: &str) -> Option<(usize, usize)> {
        let parts: Vec<&str> = macro_id.split('_').collect();
        if parts.len() == 2 {
            if let (Ok(collection_index), Ok(macro_index)) = 
                (parts[0].parse::<usize>(), parts[1].parse::<usize>()) {
                Some((collection_index, macro_index))
            } else {
                None
            }
        } else {
            None
        }
    }

    /// Creates the data directory if not present in %appdata% (only in release build).
    pub fn generate_directories() -> Result<()> {
        #[cfg(not(debug_assertions))]
        {
            let conf_dir: Result<PathBuf> = match dirs::config_dir() {
                Some(config_path) => Ok(config_path),
                None => Err(anyhow::Error::msg(
                    "Cannot find config directory, cannot proceed.",
                )),
            };

            let conf_dir = conf_dir?.join(CONFIG_DIR);

            std::fs::create_dir_all(conf_dir.as_path())?;
        }
        Ok(())
    }

    /// Sets whether the backend should process keys that it listens to. Disabling disables the processing logic, but the app still grabs the keys.
    pub fn set_is_listening(&self, is_listening: bool) {
        self.is_listening.store(is_listening, Ordering::Relaxed);
    }
    /// Sets the macros from the frontend to the files. This function is here to completely split the frontend off.
    pub async fn set_macros(&self, macros: MacroData) -> Result<()> {
        // Get currently running OnHold macros before updating data
        let running_onhold_macros = {
            let onhold_states = self.onhold_states.read().await;
            onhold_states.iter()
                .filter(|(_, is_running)| **is_running)
                .map(|(macro_id, _)| macro_id.clone())
                .collect::<Vec<String>>()
        };

        macros.write_to_file()?;
        *self.triggers.write().await = macros.extract_triggers()?;
        *self.data.write().await = macros.clone();

        // Check which running OnHold macros should be stopped due to being disabled
        let mut onhold_states = self.onhold_states.write().await;
        let mut onhold_handles = self.onhold_handles.write().await;
        
        for macro_id in running_onhold_macros {
            // Parse collection and macro indices from macro_id (format: "collection_macro")
            if let Some((collection_index, macro_index)) = self.parse_macro_id(&macro_id) {
                let should_stop = if let Some(collection) = macros.data.get(collection_index) {
                    if let Some(macro_data) = collection.macros.get(macro_index) {
                        !collection.active || !macro_data.active
                    } else {
                        true // Macro was deleted
                    }
                } else {
                    true // Collection was deleted
                };
                
                if should_stop {
                    info!("Stopping OnHold macro {} due to being disabled/deleted", macro_id);
                    onhold_states.insert(macro_id.clone(), false);
                    if let Some(handle) = onhold_handles.remove(&macro_id) {
                        handle.abort();
                    }
                }
            }
        }
        
        Ok(())
    }

    /// Sets the config from the frontend to the files. This function is here to completely split the frontend off.
    pub async fn set_config(&self, config: ApplicationConfig) -> Result<()> {
        config.write_to_file()?;
        *self.config.write().await = config;
        Ok(())
    }

    /// Initializes the entire backend and gets the whole grabbing system running.
    pub async fn init(&self) -> Result<()> {
        //? : io-uring async read files and write files
        //TODO: implement drop when the application ends to clean up the downed keys

        //==================================================

        let inner_triggers = self.triggers.clone();
        let inner_is_listening = self.is_listening.clone();
        let inner_onhold_states = self.onhold_states.clone();
        let inner_onhold_handles = self.onhold_handles.clone();
        let inner_data = self.data.clone();

        // Spawn the channels
        let (schan_execute, rchan_execute) = tokio::sync::mpsc::unbounded_channel();

        // Create the executor
        thread::spawn(move || {
            keypress_executor_sender(rchan_execute);
        });

        let _grabber = task::spawn_blocking(move || {
            let keys_pressed: KeysPressed = KeysPressed::default();

            rdev::grab(move |event: rdev::Event| {
                if inner_is_listening.load(Ordering::Relaxed) {
                    match event.event_type {
                        rdev::EventType::KeyPress(key) => {
                            debug!("Key Pressed RAW: {:?}", key);
                            let key_to_push = key;

                            let pressed_keys_copy_converted: Vec<u32> = {
                                let mut keys_pressed = keys_pressed.0.blocking_write();

                                keys_pressed.push(key_to_push);

                                *keys_pressed = keys_pressed.clone().into_iter().unique().collect();

                                keys_pressed
                                    .iter()
                                    .map(|x| *SCANCODE_TO_HID.get(x).unwrap_or(&0))
                                    .collect()
                            };

                            debug!(
                                "Pressed Keys CONVERTED TO HID:  {:?}",
                                pressed_keys_copy_converted
                            );
                            debug!(
                                "Pressed Keys CONVERTED TO RDEV: {:?}",
                                pressed_keys_copy_converted
                                    .par_iter()
                                    .map(|x| *SCANCODE_TO_RDEV
                                        .get(x)
                                        .unwrap_or(&rdev::Key::Unknown(0)))
                                    .collect::<Vec<rdev::Key>>()
                            );

                            let first_key: u32 = pressed_keys_copy_converted
                                .first()
                                .copied()
                                .unwrap_or_default();

                            let trigger_list = inner_triggers.blocking_read().clone();

                            let check_these_macros = trigger_list
                                .get(&first_key)
                                .cloned()
                                .unwrap_or_default()
                                .to_vec();

                            // ? up the pressed keys here right away?

                            let should_grab = {
                                if !check_these_macros.is_empty() {
                                    let channel_copy_send = schan_execute.clone();
                                    check_macro_execution_efficiently(
                                        pressed_keys_copy_converted,
                                        check_these_macros,
                                        channel_copy_send,
                                        inner_onhold_states.clone(),
                                        inner_onhold_handles.clone(),
                                        inner_data.clone(),
                                    )
                                } else {
                                    false
                                }
                            };

                            if should_grab {
                                None
                            } else {
                                Some(event)
                            }
                        }

                        rdev::EventType::KeyRelease(key) => {
                            keys_pressed.0.blocking_write().retain(|x| *x != key);

                            debug!("Key state: {:?}", keys_pressed.0.blocking_read());

                            // Stop OnHold macros when trigger keys are released
                            let released_key_hid = *SCANCODE_TO_HID.get(&key).unwrap_or(&0);
                            if released_key_hid != 0 {
                                let onhold_states_clone = inner_onhold_states.clone();
                                let onhold_handles_clone = inner_onhold_handles.clone();
                                let data_clone = inner_data.clone();
                                
                                task::spawn(async move {
                                    let data_read = data_clone.read().await;
                                    let mut macros_to_stop = Vec::new();
                                    
                                    // Find all OnHold macros that should stop because their trigger key was released
                                    for (collection_index, collection) in data_read.data.iter().enumerate() {
                                        if collection.active {
                                            for (macro_index, macro_item) in collection.macros.iter().enumerate() {
                                                if macro_item.active && macro_item.macro_type == MacroType::OnHold {
                                                    let should_stop = match &macro_item.trigger {
                                                        TriggerEventType::KeyPressEvent { data, .. } => {
                                                            data.contains(&released_key_hid)
                                                        },
                                                        _ => false,
                                                    };
                                                    
                                                    if should_stop {
                                                        let macro_id = generate_macro_id(collection_index, macro_index);
                                                        macros_to_stop.push(macro_id);
                                                    }
                                                }
                                            }
                                        }
                                    }
                                    
                                    // Stop all identified macros
                                    for macro_id in macros_to_stop {
                                        stop_onhold_macro(macro_id, onhold_states_clone.clone(), onhold_handles_clone.clone()).await;
                                    }
                                });
                            }

                            Some(event)
                        }

                        rdev::EventType::ButtonPress(button) => {
                            debug!("Button pressed: {:?}", button);

                            let converted_button_to_u32: u32 =
                                BUTTON_TO_HID.get(&button).unwrap_or(&0x101).to_owned();

                            let trigger_list = inner_triggers.blocking_read().clone();

                            let check_these_macros =
                                match trigger_list.get(&converted_button_to_u32) {
                                    None => {
                                        vec![]
                                    }
                                    Some(data_found) => data_found.to_vec(),
                                };

                            let channel_clone = schan_execute.clone();

                            let should_grab = check_macro_execution_efficiently(
                                vec![converted_button_to_u32],
                                check_these_macros,
                                channel_clone,
                                inner_onhold_states.clone(),
                                inner_onhold_handles.clone(),
                                inner_data.clone(),
                            );

                            // Left mouse button never gets consumed to allow users to control their PC.
                            match (should_grab, button) {
                                (true, rdev::Button::Left) => Some(event),
                                (true, _) => None,
                                (false, _) => Some(event),
                            }
                        }
                        rdev::EventType::ButtonRelease(button) => {
                            debug!("Button released: {:?}", button);

                            Some(event)
                        }
                        rdev::EventType::MouseMove { .. } => Some(event),
                        rdev::EventType::Wheel { .. } => Some(event),
                    }
                } else {
                    Some(event)
                }
            })
        });
        Err(anyhow::Error::msg("Error in grabbing thread!"))
    }
}

impl Default for MacroBackend {
    /// Generates a new state.
    fn default() -> Self {
        let macro_data =
            MacroData::read_data().unwrap_or_else(|err| panic!("Cannot get macro data! {}", err));

        let triggers = macro_data
            .extract_triggers()
            .expect("error extracting triggers");
        MacroBackend {
            data: Arc::new(RwLock::from(macro_data)),
            config: Arc::new(RwLock::from(
                ApplicationConfig::read_data().expect("error reading config"),
            )),
            triggers: Arc::new(RwLock::from(triggers)),
            is_listening: Arc::new(AtomicBool::new(true)),
            onhold_states: Arc::new(RwLock::new(HashMap::new())),
            onhold_handles: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}
