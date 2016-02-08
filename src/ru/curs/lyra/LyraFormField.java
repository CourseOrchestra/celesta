package ru.curs.lyra;

import ru.curs.celesta.CelestaException;

/**
 * Lyra form field metadata.
 */
public class LyraFormField extends LyraNamedElement {
	/**
	 * Значение по умолчанию для числа знаков после запятой.
	 */
	public static final int DEFAULT_SCALE = 0;

	private final boolean bound;
	private final transient FieldAccessor accessor;
	private LyraFieldType type;
	private boolean editable;
	private boolean visible;
	private String caption;
	private int scale = DEFAULT_SCALE;
	private int width;
	private String lookup;

	public LyraFormField(String name, boolean bound, FieldAccessor accessor) throws CelestaException {
		super(name);
		this.bound = bound;
		this.accessor = accessor;
	}

	/**
	 * Field type.
	 */
	public LyraFieldType getType() {
		return type;
	}

	/**
	 * Sets field type.
	 * 
	 * @param type
	 *            the type to set
	 */
	public void setType(LyraFieldType type) {
		this.type = type;
	}

	/**
	 * Is the field editable?
	 */
	public boolean isEditable() {
		return editable;
	}

	/**
	 * 
	 * Sets editable property.
	 * 
	 * @param editable
	 *            editable property.
	 */
	public void setEditable(boolean editable) {
		this.editable = editable;
	}

	/**
	 * Is the field visible?
	 */
	public boolean isVisible() {
		return visible;
	}

	/**
	 * Sets visible property.
	 * 
	 * @param visible
	 *            the visible to set
	 */
	public void setVisible(boolean visible) {
		this.visible = visible;
	}

	/**
	 * Caption of the field.
	 */
	public String getCaption() {
		return caption;
	}

	/**
	 * Sets new caption.
	 * 
	 * @param caption
	 *            the caption to set
	 */
	public void setCaption(String caption) {
		this.caption = caption;
	}

	/**
	 * Lookup procedure.
	 */
	public String getLookup() {
		return lookup;
	}

	/**
	 * Sets lookup procedure.
	 * 
	 * @param lookup
	 *            the lookup procedure to set
	 */
	public void setLookup(String lookup) {
		this.lookup = lookup;
	}

	/**
	 * Is the field bound?
	 */
	public boolean isBound() {
		return bound;
	}

	/**
	 * Field's getter/setter.
	 */
	public FieldAccessor getAccessor() {
		return accessor;
	}

	/**
	 * Number of decimal places after dot.
	 */
	public int getScale() {
		return scale;
	}

	/**
	 * Sets number of decimal places after dot.
	 * 
	 * @param scale
	 *            new value.
	 */
	public void setScale(int scale) {
		this.scale = scale;
	}

	/**
	 * Width (in pixels) of a control.
	 */
	public int getWidth() {
		return width;
	}

	/**
	 * Sets width (in pixels) for a control.
	 * 
	 * @param width
	 *            width in pixels.
	 */
	public void setWidth(int width) {
		this.width = width;
	}

}
