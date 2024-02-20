use std::collections::BTreeSet;
use std::fmt::Write;
use std::fs;
use std::path::Path;
use std::{collections::BTreeMap, path::PathBuf};

use anyhow::{Context, Result};
use clap::Parser;
use regex::Regex;

fn is_ovro_ms_file(entry: &Path) -> bool {
    entry.is_dir()
        && entry
            .file_name()
            .and_then(|x| x.to_str())
            .map(|s| s.trim().ends_with("MHz.ms"))
            .unwrap_or(false)
}

const FILENAME_REGEX: &str = r"(?<date>\d{8})\_\d{6}\_(?<band>\d{2})MHz\.ms";

#[derive(Parser, Debug, Clone)]
#[command(version, about)]
/// Count the Number of OVRO-LWA MS files and aggregate by date and sub-band.
/// This program will look at least 0 directory down and at most <max_depth> directories down
/// for casa MS files.
struct Args {
    /// The directory to count files in.
    #[arg(default_value = ".", required = false)]
    dirs: Vec<PathBuf>,
    /// Maximum depth to search for MS file.
    /// The execution time of this program may be heavily impacted by this.
    /// Some folders have different structures.
    /// However this program will never look inside an MS file once it is found.
    #[arg(default_value_t = 5, long)]
    max_depth: usize,
}

fn update_counter(
    entry: &Path,
    file_counter: &mut BTreeMap<String, BTreeMap<String, usize>>,
    fname_regex: &Regex,
) {
    if let Some(group) =
        fname_regex.captures(entry.file_name().and_then(|x| x.to_str()).unwrap_or(""))
    {
        match file_counter.entry(group["date"].to_owned()) {
            std::collections::btree_map::Entry::Occupied(mut entry) => {
                let inner_map = entry.get_mut();
                inner_map
                    .entry(group["band"].to_owned())
                    .and_modify(|inner_entry| *inner_entry += 1)
                    .or_insert(1);
            }
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert({
                    let mut map = BTreeMap::default();
                    map.entry(group["band"].to_owned())
                        .and_modify(|inner_entry| *inner_entry += 1)
                        .or_insert(1);
                    map
                });
            }
        };
    }
}

fn visit_dirs(
    dir: &Path,
    cb: &mut dyn FnMut(&Path),
    _depth: usize,
    max_depth: usize,
) -> Result<()> {
    if is_ovro_ms_file(dir) {
        cb(dir)
    } else if _depth == max_depth {
        return Ok(());
    } else if dir.is_dir() {
        for entry in fs::read_dir(dir)
            .context("Unable to read sub-directory")?
            .filter_map(|e| e.ok())
        {
            let e = entry.path();
            if e.is_dir() {
                visit_dirs(&e, cb, _depth + 1, max_depth)?;
            }
        }
    }
    Ok(())
}

fn consolidate_keys(
    counter: &mut BTreeMap<String, BTreeMap<String, usize>>,
    out_string: &mut String,
) {
    let keys: BTreeSet<String> = counter
        .values()
        .flat_map(|x| x.keys().map(|x| x.to_owned()))
        .collect();

    keys.iter()
        .for_each(|k| write!(out_string, " {:>6}", format!("{k:>3}MHz")).unwrap());

    counter.values_mut().for_each(|entry| {
        let inner_keys = BTreeSet::from_iter(entry.keys().map(|x| x.to_owned()));
        for new_key in keys.difference(&inner_keys) {
            entry.insert(new_key.to_string(), 0);
        }
    });
}

fn main() -> Result<()> {
    let args = Args::parse();

    let mut file_counter = BTreeMap::<String, BTreeMap<String, usize>>::default();
    let fname_regex = Regex::new(FILENAME_REGEX).expect("Malformed OVRO-LWA filename regex.");

    let mut callback = |entry: &Path| update_counter(entry, &mut file_counter, &fname_regex);

    for dir in args.dirs {
        for entry in fs::read_dir(dir)
            .context("Error reading input directory")?
            .filter_map(|e| e.ok())
        {
            visit_dirs(&entry.path(), &mut callback, 0, args.max_depth)?;
        }
    }

    let mut out_string: String = "".into();
    write!(out_string, "{:^8}", "date").unwrap();
    // it is impossible for string writes to Err  unwraps are safe.
    // see https://github.com/rust-lang/rust/blob/18bf6b4f01a6feaf7259ba7cdae58031af1b7b39/library/alloc/src/string.rs#L2414-L2427

    consolidate_keys(&mut file_counter, &mut out_string);
    writeln!(out_string).unwrap();

    for (date, counts) in file_counter {
        write!(out_string, "{date:>8}").unwrap();

        for value in counts.values() {
            write!(out_string, " {value:>6}").unwrap()
        }
        writeln!(out_string).unwrap();
    }
    println!("{out_string}");
    Ok(())
}
