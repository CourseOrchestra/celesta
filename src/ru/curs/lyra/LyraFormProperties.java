package ru.curs.lyra;

/**
 * Lyra form properties.
 */
public class LyraFormProperties {
	private String profile = null;
	private int gridwidth = -1;
	private int gridheight = -1;
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
	 * Grid width in pixels.
	 */
	public int getGridwidth() {
		return gridwidth;
	}

	/**
	 * Sets grid width.
	 * 
	 * @param gridwidth
	 *            grid width in pixels.
	 */
	public void setGridwidth(int gridwidth) {
		this.gridwidth = gridwidth;
	}

	/**
	 * Grid height in pixels.
	 */
	public int getGridheight() {
		return gridheight;
	}

	/**
	 * Sets grid height in pixels.
	 * 
	 * @param gridheight
	 *            grid height in pixels.
	 */
	public void setGridheight(int gridheight) {
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
