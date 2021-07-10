use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;

use anyhow::Result;

pub struct GoFlag {
    pub name: &'static str,
    pub desc: &'static str,
    pub is_boolean: bool,
    pub setter: Box<dyn Fn(&str) -> Result<()>>,
}

pub struct FlagValue<T> {
    r: Rc<RefCell<T>>,
}

impl<T: Clone> FlagValue<T> {
    fn new(r: Rc<RefCell<T>>) -> Self {
        Self { r }
    }

    pub fn get(&self) -> T {
        self.r.borrow().clone()
    }
}

pub struct GoFlagSet {
    flags: HashMap<&'static str, GoFlag>,
}

impl GoFlagSet {
    pub fn new() -> Self {
        GoFlagSet {
            flags: HashMap::new(),
        }
    }

    pub fn parse_args(&self, mut args: impl Iterator<Item = String>) -> Result<()> {
        let mut parsed_flags = HashSet::new();

        while let Some(arg) = args.next() {
            // Trim dashes at the beginning
            let arg = arg
                .as_str()
                .strip_prefix("--")
                .or_else(|| arg.strip_prefix('-'))
                .ok_or_else(|| anyhow::anyhow!("Expected an option, but got {}", arg))?;

            // Get the name of the flag, and - if it has form '-name=value' - its value
            let (name, maybe_value) = match arg.split_once('=') {
                Some((name, value)) => (name, Some(value)),
                None => (arg, None),
            };

            // Ensure that the flag was not present
            anyhow::ensure!(
                parsed_flags.insert(name.to_owned()),
                "The flag {} was provided twice",
                name,
            );

            // Get the flag object
            let flag = self
                .flags
                .get(&name)
                .ok_or_else(|| anyhow::anyhow!("Unknown flag: {}", name))?;

            // If the flag was not of form '-name=value`, get the value from the next arg
            let value = match maybe_value {
                Some(value) => value.to_owned(),
                None if flag.is_boolean => "1".to_owned(),
                None => args
                    .next()
                    .ok_or_else(|| anyhow::anyhow!("Value is missing for flag {}", name))?,
            };

            (flag.setter)(value.as_ref())?;
        }

        Ok(())
    }

    pub fn bool_var(
        &mut self,
        name: &'static str,
        default: bool,
        desc: &'static str,
    ) -> FlagValue<bool> {
        self.add_flag(name, true, default, desc, move |s| match s {
            "1" | "t" | "T" | "true" | "TRUE" | "True" => Ok(true),
            "0" | "f" | "F" | "false" | "FALSE" | "False" => Ok(false),
            _ => Err(anyhow::anyhow!("Invalid value for bool flag: {}", s)),
        })
    }

    pub fn string_var(
        &mut self,
        name: &'static str,
        default: impl ToString,
        desc: &'static str,
    ) -> FlagValue<String> {
        self.add_flag(
            name,
            false,
            default.to_string(),
            desc,
            |s| Ok(s.to_string()),
        )
    }

    pub fn i64_var(
        &mut self,
        name: &'static str,
        default: i64,
        desc: &'static str,
    ) -> FlagValue<i64> {
        self.add_flag(name, false, default, desc, |s| Ok(s.parse()?))
    }

    pub fn u64_var(
        &mut self,
        name: &'static str,
        default: u64,
        desc: &'static str,
    ) -> FlagValue<u64> {
        self.add_flag(name, false, default, desc, |s| Ok(s.parse()?))
    }

    pub fn var<T: Clone + 'static>(
        &mut self,
        name: &'static str,
        default: T,
        desc: &'static str,
        converter: impl Fn(&str) -> Result<T> + 'static,
    ) -> FlagValue<T> {
        self.add_flag(name, false, default, desc, converter)
    }

    fn add_flag<T: Clone + 'static>(
        &mut self,
        name: &'static str,
        is_boolean: bool,
        default: T,
        desc: &'static str,
        parser: impl Fn(&str) -> Result<T> + 'static,
    ) -> FlagValue<T> {
        let target = Rc::new(RefCell::new(default));
        let target_for_parser = target.clone();
        self.flags.insert(
            name,
            GoFlag {
                name,
                desc,
                is_boolean,
                setter: Box::new(move |s| {
                    target_for_parser.replace(parser(s)?);
                    Ok(())
                }),
            },
        );
        FlagValue::new(target)
    }
}
