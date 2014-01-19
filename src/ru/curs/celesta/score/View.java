package ru.curs.celesta.score;

/**
 * Объект-представление в метаданных.
 */
public class View extends NamedElement {

	private Grain grain;
	
	private boolean distinct;
	private Condition whereCondition;
	private TableContainer from;

	public View(Grain grain, String name) throws ParseException {
		super(name);
		if (grain == null)
			throw new IllegalArgumentException();
		this.grain = grain;
		grain.addView(this);
	}

	enum JoinType {
		INNER, LEFT, RIGHT, FULL
	}

	class TableContainer {
		private Table table;
		private String alias;
		private Join refTo;
		private Join refFrom;
	}

	class FieldContainer {
		private Column column;
		private String alias;
		private FieldContainer next;
	}

	class Join {
		private JoinType joinType;
		private TableContainer left;
		private TableContainer right;
		private Condition condition;
	}

	abstract class Condition {

	}

	enum LogicOperator {
		AND, OR
	};

	class BinaryLogicCondition extends Condition {
		private LogicOperator op;
		private Condition a;
		private Condition b;
	}

	class UnaryLogicCondition extends Condition {
		private Condition a;
	}

	enum BinaryConditionOperator {
		EQ, NOTEQ, EQGRT, GRT, EQLESS, LESS
	}

	/**
	 * Возвращает гранулу, к которой относится представление.
	 */
	public Grain getGrain() {
		return grain;
	}

}
