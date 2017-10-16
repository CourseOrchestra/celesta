package ru.curs.celesta;

import java.io.File;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

import ru.curs.celesta.score.Grain;
import ru.curs.celesta.score.Score;

/**
 * The monitor that shows the date of the most recently changed .py module.
 */
final class PythonSourceMonitor {

	private static final long POLL_INTERVAL = 10000;

	private final Runnable hook;

	private long timestamp = 0L;

	// the flat list of monitored modules
	private final List<File> modules = new ArrayList<>();
	private final List<String> moduleNames = new ArrayList<>();

	private final Timer t = new Timer(true);

	PythonSourceMonitor(Score s, Runnable hook) {
		if (hook == null)
			throw new NullPointerException();
		this.hook = hook;

		// initializing the list of monitored paths
		for (Grain g : s.getGrains().values())
			// skipping the built-in system grain
			if (!"celesta".equals(g.getName())) {
				File p = g.getGrainPath();
				if (p.isDirectory())
					addWithSubPackages(p, g.getName());
			}

		// immediate first pass
		reRead();

		// scheduling the poller
		t.schedule(new TimerTask() {
			@Override
			public void run() {
				reRead();
			}
		}, POLL_INTERVAL, POLL_INTERVAL);

	}

	private void addWithSubPackages(File p, String packageName) {
		// we are interested in python [sub]packages only!
		File f = new File(p, "__init__.py");
		if (!(f.exists() && f.isFile()))
			return;
		modules.add(f);
		moduleNames.add(packageName);

		for (String element : p.list()) {
			// in python [sub]packages we are interested in user source files
			// only!
			f = new File(p, element);
			if (f.isDirectory())
				addWithSubPackages(f, packageName + "." + element);
			else if (f.isFile() && element.endsWith(".py") && !"__init__.py".equals(element)) {
				// we don't need to monitor ORM files, but we do need to
				// reload'em.
				if (!element.endsWith("_orm.py"))
					modules.add(f);
				moduleNames.add(packageName + "." + element.substring(0, element.length() - 3));
			}
		}
	}

	private void reRead() {
		long max = timestamp;
		File freshest = null;
		for (File f : modules) {
			long lm = f.lastModified();
			if (lm > max) {
				max = lm;
				freshest = f;
			}
		}
		if (timestamp != 0L && max > timestamp) {
			Date newDate = new Date(max);
			Date oldDate = new Date(timestamp);
			System.out.printf("File timestamp change detected: '%s' --> %s, maximum timestamp was %s%n",
					freshest.toString(), newDate.toString(), oldDate.toString());
			timestamp = max;
			hook.run();
		} else {
			timestamp = max;
		}

	}

	public long getSourceTimestamp() {
		return timestamp;
	}

	public List<String> getModules() {
		return moduleNames;
	}

	public void cancel() {
		t.cancel();
	}
}
