package ru.curs.lyra;

/**
 * Lyra form properties.
 */
public class LyraFormProperties {
	private String profile = null;
	private String gridwidth = null;
	private String gridheight = null;
	private String defaultaction = null;

	/**
	 * Grid properties file.
	 */
	public String getProfile() {
		return profile;
	}

	/**
	 * Sets grid properties file.
	 * 
	 * @param profile
	 *            file name.
	 */
	public void setProfile(String profile) {
		this.profile = profile;
	}

	/**
	 * Grid width in HTML units.
	 */
	public String getGridwidth() {
		return gridwidth;
	}

	/**
	 * Sets grid width.
	 * 
	 * @param gridwidth
	 *            grid width in HTML units.
	 */
	public void setGridwidth(String gridwidth) {
		this.gridwidth = gridwidth;
	}

	/**
	 * Grid height in HTML units.
	 */
	public String getGridheight() {
		return gridheight;
	}

	/**
	 * Sets grid height in HTML units.
	 * 
	 * @param gridheight
	 *            grid height in pixels.
	 */
	public void setGridheight(String gridheight) {
		this.gridheight = gridheight;
	}

	/**
	 * Gets default action.
	 */
	public String getDefaultaction() {
		return defaultaction;
	}

	/**
	 * Sets default action.
	 * 
	 * @param defaultaction
	 *            default action.
	 */
	public void setDefaultaction(String defaultaction) {
		this.defaultaction = defaultaction;
	}

}
