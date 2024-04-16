use std::convert::TryInto;
use std::fmt::Display;
use std::fmt::Write as _;
use std::str::from_utf8;
use std::str::FromStr;
use std::time::Instant;

use absh::ansi;
use absh::duration::Duration;
use absh::experiment::Experiment;
use absh::experiment_map::ExperimentMap;
use absh::experiment_name::ExperimentName;
use absh::measure::key::MeasureKey;
use absh::measure::map::MeasureMap;
use absh::measure::tr::AllMeasures;
use absh::measure::tr::MaxRss;
use absh::measure::tr::MeasureDyn;
use absh::measure::tr::User;
use absh::measure::tr::WallTime;
use absh::mem_usage::MemUsage;
use absh::run_log::RunLog;
use absh::sh::run_sh;
use absh::sh::spawn_sh;
use clap::Parser;
use rand::prelude::SliceRandom;
use wait4::Wait4;

/// A/B testing for shell scripts.
#[derive(clap::Parser, Debug)]
struct Opts {
    /// A variant shell script.
    #[clap(short)]
    a: String,
    /// B variant shell script.
    #[clap(short)]
    b: Option<String>,
    /// C variant shell script.
    #[clap(short)]
    c: Option<String>,
    /// D variant shell script.
    #[clap(short)]
    d: Option<String>,
    /// E variant shell script.
    #[clap(short)]
    e: Option<String>,
    /// A variant warmup shell script.
    #[clap(short = 'A', long = "a-warmup")]
    aw: Option<String>,
    /// B variant warmup shell script.
    #[clap(short = 'B', long = "b-warmup")]
    bw: Option<String>,
    /// C variant warmup shell script.
    #[clap(short = 'C', long = "c-warmup")]
    cw: Option<String>,
    /// D variant warmup shell script.
    #[clap(short = 'D', long = "d-warmup")]
    dw: Option<String>,
    /// E variant warmup shell script.
    #[clap(short = 'E', long = "e-warmup")]
    ew: Option<String>,
    /// Randomise test execution order.
    #[clap(short = 'r')]
    random_order: bool,
    /// Ignore the results of the first iteration.
    #[clap(short = 'i')]
    ignore_first: bool,
    /// Stop after n successful iterations (run forever if not specified).
    #[clap(short = 'n')]
    iterations: Option<u32>,
    /// Also measure max resident set size.
    #[clap(short = 'm', long)]
    mem: bool,
    #[clap(long)]
    also_measure: Vec<AlsoMeasure>,
    /// Test is considered failed if it takes longer than this many seconds.
    #[clap(long)]
    max_time: Option<u32>,
}

#[derive(Debug)]
struct AlsoMeasure {
    id: String,
    is_size: bool,
    name: String,
    cmd: String,
}

impl FromStr for AlsoMeasure {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // expect format:  "measure-id:0:description of measure:command to run"
        if let Some((id, rest)) = s.split_once(":") {
            if let Some((is_size, rest)) = rest.split_once(":") {
                if let Some((description, cmd)) = rest.split_once(":") {
                    let is_size = match is_size {
                        "0" => false,
                        "1" => true,
                        _ => return Err(s.to_owned()),
                    };
                    return Ok(AlsoMeasure {
                        id: id.to_owned(),
                        is_size,
                        name: description.to_owned(),
                        cmd: cmd.to_owned(),
                    });
                }
            }
        }
        Err(s.to_owned())
    }
}

impl Display for AlsoMeasure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}:{}:{}", self.id, self.is_size, self.name, self.cmd)
    }
}

fn run_test(log: &mut RunLog, test: &mut Experiment, opts: &Opts) -> anyhow::Result<()> {
    writeln!(log.both_log_and_stderr())?;
    writeln!(
        log.both_log_and_stderr(),
        "running test: {}",
        test.name.name_colored()
    )?;
    let warmup_lines = test.warmup.lines().collect::<Vec<_>>();
    if !warmup_lines.is_empty() {
        writeln!(log.both_log_and_stderr(), "running warmup script:")?;
        for line in &warmup_lines {
            writeln!(log.both_log_and_stderr(), "    {}", line)?;
        }
    }

    let mut process = spawn_sh(&test.warmup)?;
    let status = process.wait4()?;
    if !status.status.success() {
        writeln!(
            log.both_log_and_stderr(),
            "warmup failed: {}",
            status.status
        )?;
        return Ok(());
    }

    writeln!(log.both_log_and_stderr(), "running script:")?;
    let lines = test.run.lines().collect::<Vec<_>>();
    for line in &lines {
        writeln!(log.both_log_and_stderr(), "    {}", line)?;
    }

    let start = Instant::now();

    let mut process = spawn_sh(&test.run)?;
    let status = process.wait4()?;

    let duration = Duration::from_nanos(start.elapsed().as_nanos().try_into()?);

    if !status.status.success() {
        writeln!(
            log.both_log_and_stderr(),
            "script failed: {}",
            status.status
        )?;
        return Ok(());
    }
    if let Some(max_time_s) = opts.max_time {
        if duration.seconds_f64() > max_time_s as f64 {
            writeln!(
                log.both_log_and_stderr(),
                "script took too long: {} s",
                duration.seconds_f64() as u64
            )?;
            return Ok(());
        }
    }

    if status.rusage.maxrss == 0 {
        return Err(anyhow::anyhow!("maxrss not available"));
    }
    let max_rss = MemUsage::from_bytes(status.rusage.maxrss);

    test.measures[MeasureKey::WallTime].push(duration.nanos());
    test.measures[MeasureKey::MaxRss].push(max_rss.bytes());

    let mut extra_info = "".to_string();
    for (u, also_measure) in opts.also_measure.iter().enumerate() {
        let output = run_sh(&also_measure.cmd)?;
        if !output.status.success() {
            writeln!(
                log.both_log_and_stderr(),
                "also_measure {} failed: {}",
                &also_measure,
                status.status
            )?;
            return Ok(());
        }
        let measure = from_utf8(&output.stdout)?.trim().parse()?;
        test.measures[MeasureKey::User(u)].push(measure);

        if also_measure.is_size {
            extra_info += &format!(
                ", {} {} MiB",
                &also_measure.id,
                MemUsage::from_bytes(measure).mib()
            )
        } else {
            extra_info += &format!(", {} {} s", &also_measure.id, Duration::from_nanos(measure))
        }
    }

    writeln!(
        log.both_log_and_stderr(),
        "{} finished in {:3} s, max rss {} MiB{}",
        test.name.name_colored(),
        duration,
        max_rss.mib(),
        extra_info
    )?;

    Ok(())
}

fn run_pair(
    log: &mut RunLog,
    opts: &Opts,
    tests: &mut ExperimentMap<Experiment>,
) -> anyhow::Result<()> {
    let mut indices: Vec<ExperimentName> = tests.keys().collect();
    if opts.random_order {
        indices.shuffle(&mut rand::thread_rng());
    }
    for &index in &indices {
        run_test(log, tests.get_mut(index).unwrap(), opts)?;
    }
    Ok(())
}

fn main() -> anyhow::Result<()> {
    let opts: Opts = Opts::parse();

    let mut log = RunLog::open();
    let user_measure_count = opts.also_measure.len();

    let mut experiments = ExperimentMap::default();
    experiments.insert(
        ExperimentName::A,
        Experiment {
            name: ExperimentName::A,
            warmup: opts.aw.clone().unwrap_or(String::new()),
            run: opts.a.clone(),
            measures: MeasureMap::new_all_default(user_measure_count),
        },
    );

    fn parse_opt_test(
        tests: &mut ExperimentMap<Experiment>,
        name: ExperimentName,
        run: &Option<String>,
        warmup: &Option<String>,
        user_measure_count: usize,
    ) {
        if let Some(run) = run.clone() {
            tests.insert(
                name,
                Experiment {
                    name,
                    warmup: warmup.clone().unwrap_or(String::new()),
                    run,
                    measures: MeasureMap::new_all_default(user_measure_count),
                },
            );
        }
    }
    parse_opt_test(
        &mut experiments,
        ExperimentName::B,
        &opts.b,
        &opts.bw,
        user_measure_count,
    );
    parse_opt_test(
        &mut experiments,
        ExperimentName::C,
        &opts.c,
        &opts.cw,
        user_measure_count,
    );
    parse_opt_test(
        &mut experiments,
        ExperimentName::D,
        &opts.d,
        &opts.dw,
        user_measure_count,
    );
    parse_opt_test(
        &mut experiments,
        ExperimentName::E,
        &opts.e,
        &opts.ew,
        user_measure_count,
    );

    eprintln!("Writing absh data to {}/", log.name().display());
    if let Some(last) = log.last() {
        eprintln!("Log symlink is {}", last.display());
    }

    log.write_args()?;

    writeln!(log.log_only(), "random_order: {}", opts.random_order)?;
    for (n, t) in experiments.iter_mut() {
        writeln!(log.log_only(), "{}.run: {}", n, t.run)?;
        if !t.warmup.is_empty() {
            writeln!(log.log_only(), "{}.warmup: {}", n, t.warmup)?;
        }
    }

    if opts.ignore_first {
        run_pair(&mut log, &opts, &mut experiments)?;

        for (_n, test) in experiments.iter_mut() {
            for numbers in test.measures.values_mut() {
                numbers.clear();
            }
        }

        writeln!(log.both_log_and_stderr(), "")?;
        writeln!(
            log.both_log_and_stderr(),
            "Ignoring first run pair results."
        )?;
        writeln!(log.both_log_and_stderr(), "Now collecting the results.")?;
        writeln!(
            log.both_log_and_stderr(),
            "Statistics will be printed after the second successful iteration."
        )?;
    } else {
        writeln!(log.both_log_and_stderr(), "")?;
        writeln!(
            log.both_log_and_stderr(),
            "{yellow}First run pair results will be used in statistics.{reset}",
            yellow = ansi::YELLOW,
            reset = ansi::RESET,
        )?;
        writeln!(
            log.both_log_and_stderr(),
            "{yellow}Results might be skewed.{reset}",
            yellow = ansi::YELLOW,
            reset = ansi::RESET,
        )?;
        writeln!(
            log.both_log_and_stderr(),
            "{yellow}Use `-i` command line flag to ignore the first iteration.{reset}",
            yellow = ansi::YELLOW,
            reset = ansi::RESET,
        )?;
    }

    let mut measures: Vec<Box<dyn MeasureDyn>> = Vec::new();
    measures.push(Box::new(WallTime));
    if opts.mem {
        measures.push(Box::new(MaxRss));
    }
    for (i, also_measure) in opts.also_measure.iter().enumerate() {
        measures.push(Box::new(User {
            is_size: also_measure.is_size,
            name: format!(
                "{} ({})",
                &also_measure.name,
                if also_measure.is_size {
                    "in megabytes"
                } else {
                    "in seconds"
                },
            ),
            id: also_measure.id.clone(),
            idx: i,
        }))
    }
    let measures = AllMeasures(measures);

    loop {
        run_pair(&mut log, &opts, &mut experiments)?;

        let min_count = experiments.values_mut().map(|t| t.runs()).min().unwrap();
        if Some(min_count) == opts.iterations.map(|n| n as usize) {
            break;
        }

        if min_count < 2 {
            continue;
        }

        writeln!(log.both_log_and_stderr(), "")?;

        let graph_full = measures.render_stats(&experiments, true)?;
        let graph_short = measures.render_stats(&experiments, false)?;

        write!(log.stderr_only(), "{}", graph_full)?;
        write!(log.log_only(), "{}", graph_short,)?;

        log.write_graph(&graph_full)?;

        measures.write_raw(&experiments, &mut log)?;
    }

    Ok(())
}
