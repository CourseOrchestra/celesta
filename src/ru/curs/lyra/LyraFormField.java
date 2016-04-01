package ru.curs.lyra;

import ru.curs.celesta.CelestaException;

/**
 * Lyra form field metadata.
 */
public class LyraFormField extends LyraNamedElement {
	/**
	 * 'Visible' property name.
	 */
	public static final String VISIBLE = "visible";
	/**
	 * 'Editable' property name.
	 */
	public static final String EDITABLE = "editable";
	/**
	 * 'Caption' property name.
	 */
	public static final String CAPTION = "caption";

	/**
	 * 'Scale' property name.
	 */
	public static final String SCALE = "scale";

	/**
	 * 'Width' property name.
	 */
	public static final String WIDTH = "width";

	/**
	 * 'Required' property name.
	 */
	public static final String REQUIRED = "required";

	/**
	 * 'Subtype' property name.
	 */
	public static final String SUBTYPE = "subtype";

	/**
	 * 'LinkId' property name.
	 */
	public static final String LINKID = "linkId";

	/**
	 * Значение по умолчанию для числа знаков после запятой.
	 */
	public static final int DEFAULT_SCALE = 0;

	private final transient FieldAccessor accessor;
	private LyraFieldType type;
	private boolean editable;
	private boolean visible;
	private boolean required;
	private String caption;
	private int scale = DEFAULT_SCALE;
	private int width;
	private String lookup;
	private String subtype;
	private String linkId;

	public LyraFormField(String name) throws CelestaException {
		super(name);
		accessor = null;
	}

	public LyraFormField(String name, FieldAccessor accessor) throws CelestaException {
		super(name);
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

	/**
	 * Is the field required.
	 */
	public boolean isRequired() {
		return required;
	}

	/**
	 * Sets required property for a field.
	 * 
	 * @param required
	 *            new value
	 */
	public void setRequired(boolean required) {
		this.required = required;
	}

	/**
	 * Returns field's subtype.
	 */
	public String getSubtype() {
		return subtype;
	}

	/**
	 * Set field's subtype. Field's subtype defines the way the field is being
	 * shown to the user.
	 * 
	 * @param subtype
	 *            new subtype.
	 */
	public void setSubtype(String subtype) {
		this.subtype = subtype;
	}

	/**
	 * Gets field's linkID.
	 */
	public String getLinkId() {
		return linkId;
	}

	/**
	 * Set field's linkId.
	 * 
	 * @param linkId
	 *            new linkId.
	 */
	public void setLinkId(String linkId) {
		this.linkId = linkId;
	}

}
