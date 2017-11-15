package ru.curs.celesta;

import java.io.File;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import org.python.core.Py;
import org.python.core.PyObject;
import org.python.core.PyString;
import org.python.core.PySystemState;
import org.python.core.codecs;
import org.python.util.PythonInterpreter;

import ru.curs.celesta.score.Grain;
import ru.curs.celesta.score.Score;

/**
 * The pool of Jython interpreters.
 */
public class PythonInterpreterPool {

	private final Celesta celesta;
	private final Score score;
	private final String javaLibPath;
	private final String pyLibPath;
	private final PySystemState sysState;
	private final PythonSourceMonitor sourceMonitor;
	private final AtomicInteger activeInterpetersCount = new AtomicInteger();
	private final AtomicBoolean isUpdating = new AtomicBoolean();
	private final ConcurrentLinkedQueue<PythonInterpreter> interpreterPool = new ConcurrentLinkedQueue<>();
	private final ReentrantLock lock = new ReentrantLock();

	private class PooledPythonInterpreter extends PythonInterpreter {
		PooledPythonInterpreter(PyObject dict, PySystemState systemState) {
			super(dict, sysState);
		}

		@Override
		public void close() {
			activeInterpetersCount.decrementAndGet();
			interpreterPool.add(this);
		}
	}

	public PythonInterpreterPool(Celesta celesta, Score score, String javaLibPath, String pyLibPath) throws CelestaException {
		this.celesta = celesta;
		this.score = score;
		this.javaLibPath = javaLibPath;
		this.pyLibPath = pyLibPath;

		List<String> pyPathList = new ArrayList<>();
		initPyPathList(pyPathList);
		sysState = Py.getSystemState();

		for (String path : pyPathList) {
			PyString ppath = new PyString(path);
			if (!sysState.path.contains(ppath))
				sysState.path.append(ppath);
		}

		sourceMonitor = new PythonSourceMonitor(score, () -> {
			// if already updating -- ignore
			if (!isUpdating.compareAndSet(false, true))
				return;
			lock.lock();
			try {
				System.out.println("Source change, phase 1/2: Execution locked, waiting for Python tasks to complete.");
				while (activeInterpetersCount.get() > 0) {
					try {
						Thread.sleep(1000);
					} catch (InterruptedException e) {
						// do nothing, interrupt refreshing
						e.printStackTrace();
						return;
					}
				}
				System.out.println("Source change, phase 2/2: Reloading modules.");

				try (PythonInterpreter interp = new PythonInterpreter(null, sysState)) {
					unloadModules(interp);
					try {
						initPythonScore(interp);
					} catch (Exception e) {
						// do nothing, all info is already in stderr
					}
				}
				System.out.println("Source change done.");
			} finally {
				lock.unlock();
				isUpdating.set(false);
			}
		});

		activeInterpetersCount.incrementAndGet();
		try (PythonInterpreter interp = new PooledPythonInterpreter(null, sysState)) {
			codecs.setDefaultEncoding("UTF-8");
			initPythonScore(interp);
		}
	}

	private void unloadModules(PythonInterpreter interp) {
		interp.exec("import sys");
		for (String moduleName : sourceMonitor.getModules()) {
			interp.exec(String.format("sys.modules.pop('%s', None)", moduleName));
		}
	}

	public PythonInterpreter getPythonInterpreter() throws CelestaException {
		// wait here in case it's locked for maintenance
		lock.lock();
		lock.unlock();

		PythonInterpreter result = interpreterPool.poll();
		if (result == null) {
			result = new PooledPythonInterpreter(null, sysState);
			codecs.setDefaultEncoding("UTF-8");
			reImportGrains(result);
		}
		activeInterpetersCount.incrementAndGet();
		return result;
	}

	private void initPyPathList(List<String> pyPathList) {
		if (!javaLibPath.isEmpty())
			for (String entry : javaLibPath.split(File.pathSeparator)) {
				File f = new File(entry);
				if (f.exists() && f.isDirectory() && f.canRead()) {
					for (String filename : f.list()) {
						if (!filename.toLowerCase().endsWith(".jar"))
							continue;
						File pathEntry = new File(f, filename);
						pyPathList.add(pathEntry.getAbsolutePath());
					}
				}
			}

		File pathEntry = new File(Celesta.getMyPath() + "pylib");
		if (pathEntry.exists() && pathEntry.isDirectory()) {
			pyPathList.add(pathEntry.getAbsolutePath());
		}
		for (String entry : pyLibPath.split(File.pathSeparator)) {
			pathEntry = new File(entry);
			if (pathEntry.exists() && pathEntry.isDirectory()) {
				pyPathList.add(pathEntry.getAbsolutePath());
			}
		}
		for (String entry : score.getPath().split(File.pathSeparator)) {
			pathEntry = new File(entry.trim());
			if (pathEntry.exists() && pathEntry.isDirectory()) {
				pyPathList.add(pathEntry.getAbsolutePath());
			}
		}
	}

	private void reImportGrains(PythonInterpreter interp) throws CelestaException {
		for (Grain g : score.getGrains().values())
			if (!"celesta".equals(g.getName())) {
				try {
					String line = String.format("import %s", g.getName());
					interp.exec(line);

				} catch (Throwable e) {
					System.out.println("Python interpreter initialization error:");
					e.printStackTrace(System.out);
					throw new CelestaException(
							"Python interpreter initialization error: '%s' while importing grain %s. See stdout for details.",
							e.getMessage(), g.getName());
				}
			}
	}

	private void initPythonScore(PythonInterpreter interp) throws CelestaException {
		SessionContext scontext = new SessionContext("super", "celesta_init");

		try (CallContext context = celesta.callContext(scontext)) {
			try {
				interp.exec("import sys");
				interp.set("_ic", context);
				interp.exec("sys.modules['initcontext'] = lambda: _ic");
			} catch (Throwable e) {
				System.out.println("Python interpreter initialization error:");
				e.printStackTrace(System.out);
				throw new CelestaException("Python interpreter initialization error: %s. See stdout for details.",
						e.getMessage());
			}
			reImportGrains(interp);
		}
		interp.set("_ic", "You can't use initcontext() outside the initialization code!");
	}

	void cancelSourceMonitor() {
		sourceMonitor.cancel();
	}
}
