package ru.curs.celesta.score;

/**
 * Ссылка на таблицу в SQL-запросе.
 */
public class TableRef {
	/**
	 * Тип JOIN. Не поддерживается FULL JOIN из исторических соображений,
	 * возможно когда-нибудь будет добавлен
	 * , и CROSS JOIN из соображений безопасности быстродействия.
	 */
	public enum JoinType {

		/**
		 * INNER JOIN.
		 */
		INNER, /**
		 * LEFT JOIN.
		 */
		LEFT, /**
		 * RIGHT JOIN.
		 */
		RIGHT
	}

	private final Table table;
	private final String alias;

	private JoinType joinType;
	private Expr onExpr;

	public TableRef(Table table, String alias) {
		this.table = table;
		this.alias = alias;
	}

	/**
	 * Тип JOIN.
	 */
	public JoinType getJoinType() {
		return joinType;
	}

	/**
	 * Таблица.
	 */
	public Table getTable() {
		return table;
	}

	/**
	 * Условие ON...
	 */
	public Expr getOnExpr() {
		return onExpr;
	}

	void setOnExpr(Expr onExpr) throws ParseException {
		if (onExpr == null)
			throw new IllegalArgumentException();
		onExpr.assertType(ViewColumnType.LOGIC);
		this.onExpr = onExpr;

	}

	/**
	 * Псевдоним таблицы.
	 */
	public String getAlias() {
		return alias;
	}

	void setJoinType(JoinType joinType) {
		this.joinType = joinType;
	}
}