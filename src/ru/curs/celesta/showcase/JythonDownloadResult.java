package ru.curs.celesta.showcase;

import java.io.InputStream;

/**
 * Результат выполнение Jython скрипта, загрузка файла из компонента grid.
 * 
 * @author anlug
 * 
 */
public class JythonDownloadResult {
	private final InputStream inputStream;
	private final String fileName;
	private final JythonErrorResult error;

	public JythonDownloadResult(final InputStream aInputStream, final String aFileName) {
		super();
		this.inputStream = aInputStream;
		this.fileName = aFileName;
		this.error = null;
	}

	public JythonDownloadResult(final JythonErrorResult aError) {
		super();
		this.inputStream = null;
		this.fileName = null;
		this.error = aError;
	}

	public JythonDownloadResult(final InputStream aInputStream, final String aFileName,
			final JythonErrorResult aError) {
		super();
		this.inputStream = aInputStream;
		this.fileName = aFileName;
		this.error = aError;
	}

	public InputStream getInputStream() {
		return inputStream;
	}

	public String getFileName() {
		return fileName;
	}

	public JythonErrorResult getError() {
		return error;
	}
}
