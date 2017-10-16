package ru.curs.lyra;

import java.util.Optional;
import java.util.function.Function;

/**
 * Lyra form properties.
 */
public class LyraFormProperties {
	private Optional<String> profile = Optional.empty();
	private Optional<String> gridwidth = Optional.empty();
	private Optional<String> gridheight = Optional.empty();
	private Optional<String> defaultaction = Optional.empty();
	private Optional<String> footer = Optional.empty();
	private Optional<String> header = Optional.empty();

	private final Optional<LyraFormProperties> parent;

	public LyraFormProperties() {
		this.parent = Optional.empty();
	}

	public LyraFormProperties(LyraFormProperties parent) {
		this.parent = Optional.ofNullable(parent);
	}

	private <T> T getValue(Optional<T> val, Function<LyraFormProperties, T> f) {
		return val.orElse(parent.map(f).orElse(null));
	}

	/**
	 * Grid properties file.
	 */
	public String getProfile() {
		return getValue(profile, LyraFormProperties::getProfile);
	}

	/**
	 * Sets grid properties file.
	 * 
	 * @param profile
	 *            file name.
	 */
	public void setProfile(String profile) {
		this.profile = Optional.ofNullable(profile);
	}

	/**
	 * Grid width in HTML units.
	 */
	public String getGridwidth() {
		return getValue(gridwidth, LyraFormProperties::getGridwidth);
	}

	/**
	 * Sets grid width.
	 * 
	 * @param gridwidth
	 *            grid width in HTML units.
	 */
	public void setGridwidth(String gridwidth) {
		this.gridwidth = Optional.ofNullable(gridwidth);
	}

	/**
	 * Grid height in HTML units.
	 */
	public String getGridheight() {
		return getValue(gridheight, LyraFormProperties::getGridheight);
	}

	/**
	 * Sets grid height in HTML units.
	 * 
	 * @param gridheight
	 *            grid height in pixels.
	 */
	public void setGridheight(String gridheight) {
		this.gridheight = Optional.ofNullable(gridheight);
	}

	/**
	 * Gets default action.
	 */
	public String getDefaultaction() {
		return getValue(defaultaction, LyraFormProperties::getDefaultaction);
	}

	/**
	 * Sets default action.
	 * 
	 * @param defaultaction
	 *            default action.
	 */
	public void setDefaultaction(String defaultaction) {
		this.defaultaction = Optional.ofNullable(defaultaction);
	}

	/**
	 * Gets form's footer.
	 */
	public String getFooter() {
		return getValue(footer, LyraFormProperties::getFooter);
	}

	/**
	 * Set form's footer.
	 * 
	 * @param footer
	 *            new form's footer.
	 */
	public void setFooter(String footer) {
		this.footer = Optional.ofNullable(footer);
	}

	/**
	 * Gets form's header.
	 * 
	 * @return form's header.
	 */
	public String getHeader() {
		return getValue(header, LyraFormProperties::getHeader);
	}

	/**
	 * Sets form's header.
	 * 
	 * @param header
	 *            new form's header.
	 */
	public void setHeader(String header) {
		this.header = Optional.ofNullable(header);
	}

}
