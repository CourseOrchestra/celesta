package ru.curs.celesta;

/**
 * Контекст системы Showcase, присоединяемый к вызову Celesta.
 */
public final class ShowcaseContext {
	private final String main;
	private final String additional;
	private final String filter;
	private final String session;
	private final String elementId;

	private String[] orderBy = null;

	public ShowcaseContext(String main, String additional, String filter, String session,
			String elementId) {
		this.main = main;
		this.additional = additional;
		this.filter = filter;
		this.session = session;
		this.elementId = elementId;
	}

	public String getMain() {
		return main;
	}

	public String getAdditional() {
		return additional;
	}

	public String getFilter() {
		return filter;
	}

	public String getSession() {
		return session;
	}

	public String getElementId() {
		return elementId;
	}

	public String[] getOrderBy() {
		return orderBy;
	}

	public void setOrderBy(final String[] aOrderBy) {
		orderBy = aOrderBy;
	}

}
