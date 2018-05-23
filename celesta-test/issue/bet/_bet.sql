CREATE GRAIN bet VERSION '1.81';

-- *** TABLES ***
/**Таблица для глобальных переменных*/
CREATE TABLE Setup(
  id INT NOT NULL DEFAULT 0,
  /* Значения лимитов */
  bet_level_1 REAL DEFAULT 0.0,
  bet_level_2 REAL DEFAULT 0.0,
  bet_level_3 REAL DEFAULT 100.0,
  marginality REAL,
  /*Таймаут перед приёмом ставки*/
  bet_timeout REAL,
  CONSTRAINT Pk_Setup PRIMARY KEY (id)
);

/* Таблица для логов изменения (пока) уровня доверия
CREATE TABLE TrustChangesLog(
  id INT NOT NULL IDENTITY,
  employee_id VARCHAR(30) NOT NULL,
  bettor_id VARCHAR(30) NOT NULL,
  transaction_time DATETIME NOT NULL DEFAULT GETDATE(),
  from_level

  CONSTRAINT Pk_tr_ch_log PRIMARY KEY (id)
);
*/

/*Таблица записи соответствий между транзакциями*/
CREATE TABLE Register(
  id INT NOT NULL IDENTITY,
  employee VARCHAR(50),
  bettor VARCHAR(50),
  posting_date DATETIME NOT NULL DEFAULT GETDATE(),
  CONSTRAINT Pk_Register PRIMARY KEY (id)
);

/**Тип операции:
1 взнос средств
2 ставка
3 начисление выигрыша
4 выплата*/
CREATE TABLE BettorLEntryType(
  code VARCHAR(30) NOT NULL,
  description VARCHAR(50),
  CONSTRAINT Pk_EntryType_0 PRIMARY KEY (code)
);

/**Движение по счетам игроков*/
CREATE TABLE BettorLedgerEntry(
  entry_no INT NOT NULL IDENTITY,
  entry_type VARCHAR(30) NOT NULL,
  /**момент времени, в который учтено движение средств
  {caption: "Время появления транзакции"} */
  posting_date DATETIME NOT NULL DEFAULT GETDATE(),
  /**Номер документа-основания для операции (ПКО, купон и т. п.)
  {caption: "Номер документа"} */
  document_no VARCHAR(30),
  /**Дата документа-основания для проводки
  {caption: "Дата оформления документа"} */
  document_date DATETIME,
  bettor VARCHAR(50) NOT NULL,
  /**связанная с операцией ставка (если есть, null если нет)
  {caption: 'ID ставки'}*/
  related_booking INT,
  /**Сумма транзакции
  {caption: "Сумма"}*/
  amount REAL NOT NULL DEFAULT 0.0,
  /** валюта */
  currency INT,
  payment_method VARCHAR(30) NOT NULL,
  register_id INT NOT NULL,
  CONSTRAINT Pk_GamblerLedger PRIMARY KEY (entry_no)
) WITH NO VERSION CHECK;

/* Таблица данных об игре */
CREATE TABLE LogGame(
  id INT NOT NULL,
  event_id INT NOT NULL,
  value_cur VARCHAR(255),
  value_old VARCHAR(255),
  time_stamp DATETIME,
  CONSTRAINT Pk_LogGame PRIMARY KEY (id)
);

/**Ставка*/
CREATE TABLE Booking(
  booking_no INT NOT NULL IDENTITY,
  /**момент времени, в который учтена ставка
  {caption: "Добавлена"}*/
  posting_date DATETIME NOT NULL DEFAULT GETDATE(),
  /**{caption: "Статус", option: [BETTED,LOST,WON,CANCELED]}*/
  status INT NOT NULL DEFAULT 0,
  bettor VARCHAR(50) NOT NULL,
  point VARCHAR(30) NOT NULL,
  /**коэффициент принятой ставки
  {caption: "Коэффициент", width: 80}*/
  quotient REAL NOT NULL,
  /**сумма сделанной ставки
  {caption: "Сумма", width: 140} */
  amount REAL NOT NULL DEFAULT 0.0,
  /** валюта */
  currency INT,
  time_stamp DATETIME,
  CONSTRAINT Pk_Bet PRIMARY KEY (booking_no)
);

/**Группа исходов, на которую делается ставка*/
CREATE TABLE BookingOutcomes(
  id INT NOT NULL IDENTITY,
  booking_no INT NOT NULL,
  outcome INT NOT NULL,
  event INT NOT NULL,
  /**Коэффициент, с которым была принята ставка*/
  odds REAL NOT NULL DEFAULT 1.0,
  CONSTRAINT Idx_BookingOutcomeId PRIMARY KEY (id)
);


CREATE TABLE Country(
	id INT NOT NULL,
	code2 VARCHAR(2),
	code3 VARCHAR(3),
	name VARCHAR(50),
	CONSTRAINT Pk_Country PRIMARY KEY (id)
);

CREATE TABLE Currency(
	numeric_code INT NOT NULL,
	alphabetic_code VARCHAR(3),
	currency VARCHAR(255),
	CONSTRAINT Pk_Currency PRIMARY KEY (numeric_code)
);

CREATE TABLE City(
  code VARCHAR(30) NOT NULL,
  name VARCHAR(100) NOT NULL,
  CONSTRAINT Pk_City PRIMARY KEY (code)
);

/**Чемпионаты, турниры, матчи*/
CREATE TABLE Category(
  /**{caption: "Код"} */
  code VARCHAR(50) NOT NULL,

  /*{caption: "Иерархия"}
  dew_id VARCHAR(50) NOT NULL,
  dew_sort VARCHAR(100) NOT NULL,
  */

  /**вид спорта*/
  sport INT NOT NULL,
  /**{caption: "Наименование"}*/
  name VARCHAR(100) NOT NULL,
  /**{caption: "Полное наименование"}*/
  full_name VARCHAR(400),
  /**{caption: "Полное наименование (EN)"}*/
  full_name_en VARCHAR(400),
  /**
  {option:[PLANNED,LIVE,COMPLETE,CANCELED], caption: "Статус"}*/
  status INT NOT NULL DEFAULT 0,
  /* Последний источник данных марафон или 1xBET */
  last_source INT NOT NULL DEFAULT 0,
  /**{caption: "Заблокирован"}*/
  blocked BIT NOT NULL DEFAULT 0,
  is_deleted BIT NOT NULL DEFAULT 0,
  /**{caption: "Лимит"} */
  limit REAL,
  /**{caption: "КМ"}*/
  marginality REAL,
  CONSTRAINT Pk_Category PRIMARY KEY (code)
);


/**матчи, гонки чемпионата, а также специальные события вроде "победителя чемпионата"*/
CREATE TABLE Event(
  /**{caption: "Код"}*/
  id INT IDENTITY NOT NULL,
  /**{caption: "Категория"}*/
  category VARCHAR(50) NOT NULL,
  /**{caption: "Наименование"}*/
  name VARCHAR(255) NOT NULL,
  /**{caption: "Наименование (EN)"}*/
  name_en VARCHAR(255),
  /**Дата и время события*/
  dt DATETIME,
  /**{option:[PLANNED,LIVE,COMPLETE,CANCELED],
  * caption: "Статус"}*/
  status INT NOT NULL DEFAULT 0,
  /* Последний источник данных марафон или 1xBET */
  last_source INT NOT NULL DEFAULT 0,
  /**{caption: "Заблокирован"}*/
  blocked BIT NOT NULL DEFAULT 0,
  /**{caption: "Лимит"} */
  limit REAL,
  is_deleted BIT NOT NULL DEFAULT 0,
  /**{caption: "КМ"}*/
  marginality REAL,
  /*Вид спорта, к которому относится событие*/
  sport_id INT NOT NULL,
  CONSTRAINT Pk_Match PRIMARY KEY (id)
);

/**Сотрудник*/
CREATE TABLE Employee(
  code VARCHAR(50) NOT NULL,
  CONSTRAINT Pk_Employee PRIMARY KEY (code)
);


/**Счёт операции:
1 Депонированные средства
2 Собственные средства*/
CREATE TABLE GLAccount(
  code VARCHAR(30) NOT NULL,
  description VARCHAR(50),
  CONSTRAINT Pk_EntryType_1 PRIMARY KEY (code)
);

/**Тип операции:
1 приход средств (сделанные ставки)
2 выплата выигрыша
3 выплата хоз расходов
4 внутренний перевод средств
5 вывод средств
6 ввод средств*/
CREATE TABLE GenLEntryType(
  code VARCHAR(30) NOT NULL,
  description VARCHAR(50),
  CONSTRAINT Pk_EntryType PRIMARY KEY (code)
);

/**Движение средств по счетам предприятия*/
CREATE TABLE GeneralLedgerEntry(
  entry_no INT NOT NULL IDENTITY,
  entry_type VARCHAR(30) NOT NULL,
  /**момент времени, в который учтено движение средств*/
  posting_date DATETIME NOT NULL DEFAULT GETDATE(),
  /**Номер документа-основания для проводки*/
  document_no VARCHAR(30),
  /**дата документа-основания для проводки*/
  document_date DATETIME,
  /**ППС*/
  point VARCHAR(30) NOT NULL,
  /**`*/
  account VARCHAR(30) NOT NULL,
  /**связанная с операцией ставка (если есть), null если нет.*/
  related_booking INT,
  /**Сумма (положительная или отрицательная)*/
  amount REAL NOT NULL DEFAULT 0.0,
  /** валюта */
  currency INT NOT NULL,
  /**Текстовое описание проводки*/
  description VARCHAR(200),
  payment_method VARCHAR(30) NOT NULL,
  register_id INT NOT NULL,
  CONSTRAINT Pk_Ledger PRIMARY KEY (entry_no)
) WITH NO VERSION CHECK;

/**Исход события, на который можно сделать ставку*/
CREATE TABLE Outcome(
  id INT IDENTITY NOT NULL,
  sport_id INT NOT NULL,
  code VARCHAR(128) NOT NULL,
  CONSTRAINT Pk_Outcome PRIMARY KEY (id)
) WITH NO VERSION CHECK;


CREATE TABLE GameState(
    id INT IDENTITY NOT NULL,
    event INT,
    source INT NOT NULL,
    data VARCHAR(128),
    time_stamp DATETIME,

    CONSTRAINT Pk_GameState PRIMARY KEY (id)
);

/**Текущие коэффициенты по событиям*/
CREATE TABLE OutcomeOdds(
  event INT NOT NULL,
  outcome INT NOT NULL,
  odds REAL,
  odds_old REAL,
  /*{option:[NOT_IMPLEMENTED,WON,LOST,CANCELED]}*/
  status INT NOT NULL,
  /*Состояние игры, которое привело событие в статус*/
  game_state INT,
  /* Последний источник данных марафон или 1xBET */
  last_source INT NOT NULL DEFAULT 0,
  time_stamp DATETIME NOT NULL DEFAULT GETDATE(),
  is_deleted BIT NOT NULL DEFAULT 0,
  --outcome_code VARCHAR(128) NOT NULL,
  CONSTRAINT Pk_OutcomeOdds PRIMARY KEY (event, outcome)
);


CREATE VIEW BookingOutcomesState AS
	SELECT bo.booking_no, bo.event, bo.outcome, bo.odds, oo.status, oo.game_state, b.status as bet_status
	FROM bet.BookingOutcomes AS bo
		INNER JOIN bet.OutcomeOdds AS oo ON bo.event = oo.event AND bo.outcome = oo.outcome
		INNER JOIN bet.Booking AS b on b.booking_no = bo.booking_no;

/*Журнал изменений коэффициентов на исходы событий.
 * Сюда записываются коэффициенты с применёнными модификаторами.
 * */
CREATE TABLE OutcomeOddsHistory(
  id INT NOT NULL IDENTITY,
  event INT NOT NULL,
  outcome INT NOT NULL,
  odds REAL,
  odds_old REAL,
  /*{option:[NOT_IMPLEMENTED,WON,LOST,CANCELED]}*/
  status INT NOT NULL,
  last_source INT NOT NULL DEFAULT 0,
  time_stamp DATETIME NOT NULL,
  is_deleted BIT NOT NULL DEFAULT 0,
  CONSTRAINT Pk_OutcomeOddsHistory PRIMARY KEY (id)
) WITH NO VERSION CHECK;


CREATE TABLE OutputMessage(
  entry_no INT NOT NULL IDENTITY,
  person VARCHAR(50),
  msg_text VARCHAR(250),
  /**{option: [SMS,EMAIL]}*/
  type INT,
  address VARCHAR(100),
  put_timestamp DATETIME NOT NULL DEFAULT GETDATE(),
  sent_timestamp DATETIME,
  /**{option: [NEW, IN_PROGRESS, SENT, FAIL]}*/
  status INT,
  errortext TEXT,
  result BLOB,
  script VARCHAR(255),
  parameters VARCHAR(255),
  CONSTRAINT Pk_Message PRIMARY KEY (entry_no)
);

CREATE TABLE PaymentMethod(
  code VARCHAR(30) NOT NULL,
  description VARCHAR(50),
  CONSTRAINT Pk_PaymentMethod PRIMARY KEY (code)
);

/**Игрок*/
CREATE TABLE Person(
    code VARCHAR(50) NOT NULL,
    /** валюта */
    currency INT,
	/**лимит ставки в рублях, которую может сделать игрок
 	{caption: "Лимит"}*/
    limit REAL,
	/**тип порезки
	{option: [MONEY,PROCENT,COEFFICIENT]}*/
    limit_type INT,

    country INT,
	/**Определяет, является ли игрок привилегированным
	{option: [wary,normal,vip]}*/
    kind INT,
    is_blocked BIT NOT NULL DEFAULT 0,
    registration_date DATETIME DEFAULT GETDATE(),
    main_account_code VARCHAR(30),
	/**{caption: "Имя"}*/
    first_name VARCHAR(100) NOT NULL,
	/**{caption: "Фамилия"}*/
    second_name VARCHAR(100),
	/**{caption: "Отчество"}*/
    family_name VARCHAR(100),
	/**{caption: "Пол", option: [MALE, FEMALE, NOT_SPECIFIED]}*/
    sex INT,
    /**{caption: "Дата рождения (возраст)"}*/
    born_date DATETIME,
    /**Уровень подтверждения аккаунта
	L0. new. Заполнены данные на фронте, не подтверждено
	L1. basic. 3аполнено ФИО и подтвержден по крайней мере один из следующих атрибутов: email, номер мобильного телефона, аккаунт в социальной сети.
	L2. self-identified. Пользователь самостоятельно заполнил полную анкету с личными данными, но эти данные ещё не были верифицированы риск-менеджером.
	L3. identified. Риск-менеджер подтвердил, что в системе имеется качественная скан-копия паспорта (главной страницы и прописки) пользователя (принимается паспорт той страны,
	гражданином которой является игрок) и на основании данной копии заполнены следующие поля: ФИО, дата рождения, номер паспорта, дата выдачи паспорта.
	L4. verified. Произведена личная верификация игрока: по скайп-конференции либо непосредственно в пункте приема ставок.
	{caption: "Уровень доверия", option:[NEW,BASIC,SELF_IDENTIFIED,IDENTIFIED,VERIFIED]}*/
    trust_level INT,
    /*
    {caption: "Способ подтверждения", option:[SKYPE,VIZIT,PHOTO,OTHER]}*/
    vizit_type INT,
    /*{caption: "Политика подтверждения ставок"}*/
    bet_acceptance_policy INT,
    /**электронная почта
    {caption: "Email"}*/
    email VARCHAR(50),
    /**Подтверждение e-mail
    {caption: "e-mail подтвержден"}*/
    email_confirmed BIT NOT NULL DEFAULT 0,
	/**мобильный телефон
	{caption: "Моб. номер"}*/
    mobile VARCHAR(20),
    /**Подтверждение номера телефона
	{caption: "Телефон подтвержден"}*/
    mobile_confirmed BIT NOT NULL DEFAULT 0,
	/**логин в скайпе
	{caption: "Скайп"}*/
    skype VARCHAR(50),
	/**номер паспорта
	{caption: "Номер паспорта"}*/
    passport_no VARCHAR(20),
    /**Дата выдачи паспорта*/
    passport_date DATETIME,
    /**скан-копия паспорта */
    passport_scan BLOB,
    /** регистрация */
    registration VARCHAR(300),
    /** кем выдан */
    passport_issued VARCHAR(300),
    CONSTRAINT Pk_Person PRIMARY KEY (code)
);

/**Таблица хранения токенов*/
CREATE TABLE ConfirmationToken(
    /** Код игрока */
    bettor VARCHAR(50) NOT NULL,
    /** ИД средства связи */
    communication_tool INT NOT NULL,
    /** Токен. На текущий момент – строковое представление инта. */
    token VARCHAR(20) NOT NULL,
    /** Дата формирования токена (фактически – дата записи токена в таблицу) */
    time_stamp DATETIME,
    /** Время окончания действия токена*/
    expire_date	DATETIME,

    CONSTRAINT pk_confirmation_token PRIMARY KEY (bettor, communication_tool)
);

/* Личный счёт игрока */
CREATE TABLE BettorAccount(
    code VARCHAR(50) NOT NULL,
    /**{caption: "Баланс"}*/
    balance REAL DEFAULT 0.0,

    CONSTRAINT pk_bettor_account PRIMARY KEY (code)
);


/**Пункт приема ставок, ППС*/
CREATE TABLE Point(
  code VARCHAR(30) NOT NULL,
  city VARCHAR(30) NOT NULL,
  /**адрес ППС*/
  address VARCHAR(200),
  /**месячный лимит расходов на хозоперации*/
  limit_amount REAL NOT NULL DEFAULT 0.0,
  CONSTRAINT Pk_Point PRIMARY KEY (code)
);

/* Отображение таблицы Point в Employee */
CREATE TABLE PointEmployeeMapping(
  code VARCHAR(50) NOT NULL,
  point_code VARCHAR(30) NOT NULL,
  employee_code VARCHAR(50) NOT NULL,
  is_manager BIT NOT NULL DEFAULT 0,

  CONSTRAINT Pk_PEM PRIMARY KEY (code)
);

/**Виды спорта*/
CREATE TABLE Sport(
  /**код*/
  id INT NOT NULL,
  /**наименование*/
  name VARCHAR(100) NOT NULL,
  CONSTRAINT Pk_Sport PRIMARY KEY (id)
);

/**Заявка на выплату выигрыша*/
CREATE TABLE WithdrawalRequest(
  entry_no INT IDENTITY NOT NULL,
  /** Тип платёжной системы
  {caption: "ПС"} */
  ps INT,
  bettor VARCHAR(50) NOT NULL,
  /** IP пользователя
  {caption: 'IP'} */
  bettor_ip VARCHAR(255),
  /**{caption: "Сумма"}*/
  amount REAL NOT NULL,
  /**{caption: "Валюта"}*/
  currency INT,
  /**{caption: "Время подачи заявки"}*/
  creation_date DATETIME NOT NULL,
  /**{caption: "Статус", option: [new,confirmed,risk_assesment,risk_approved,approved,rejected,paid]}*/
  status INT NOT NULL DEFAULT 0,
  /**{caption: "Время последней обработки заявки"}*/
  processing_date DATETIME,
  /**{caption: "Способ оплаты"}*/
  payment_method VARCHAR(30),
  /*TODO: удалить, т.к. везде для обозначения сотрудников используется employee*/
  processed_by VARCHAR(50),
  employee VARCHAR(50),
  /**{caption: "Комментарий"}*/
  comment TEXT,
  CONSTRAINT Pk_WithdrawalRequest PRIMARY KEY (entry_no)
);

/**История изменений заявки на выплату*/
CREATE TABLE WithdrawalRequestHistory(
  entry_no INT NOT NULL IDENTITY,
  request INT NOT NULL,
  processing_date DATETIME NOT NULL,
  /**сотрудник, внесший изменение*/
  employee VARCHAR(50),
  old_status INT NOT NULL,
  new_status INT NOT NULL,
  comment TEXT,
  CONSTRAINT Pk_WithdrawalRequestHistory PRIMARY KEY (entry_no)
) WITH NO VERSION CHECK;

CREATE TABLE Document(
	/** Идентификатор документа
	{caption: 'ИД'} */
    document_id INT NOT NULL IDENTITY,
    /** Тип платёжной системы
	{caption: 'ПС'} */
    ps INT NOT NULL,
    /** Тип операции
	{caption: 'Операция'} */
    operation INT NOT NULL,
    /** Сумма операции
	{caption: 'Сумма'} */
    amount REAL NOT NULL,
    /** ИД валюты операции (ISO)
	{caption: 'Валюта'} */
    currency INT NOT NULL,
    /** Статус
	{caption: 'Статус'} */
    status INT NOT NULL,
    /** Дата и время создания документа
	{caption: 'Дата создания'} */
    date_created DATETIME NOT NULL,
    /** Дата и время последнего изменения документа
	{caption: 'Дата изменения'} */
    date_modified DATETIME NOT NULL,
    /** ИД транзакции ПС
	{caption: 'ИД транзакции'} */
    transaction VARCHAR(255),
    /** Сотрудник, инициировавший транзакцию
	{caption: 'Сотрудник'} */
    employee VARCHAR(50),
    /** Игрок, инициировавший транзакцию
	{caption: 'Игрок'} */
    bettor VARCHAR(50),
    /** Причина создания документа
	{caption: 'Основание'} */
    cause VARCHAR(255),
    /** ИД записи в регистре, с которой связан этот документ
	{caption: 'Запись в регистре'} */
    register_id INT,
    /** Сообщение-комментарий к последнему изменению
	{caption: 'Сообщение'} */
    message TEXT,
    CONSTRAINT pk_document PRIMARY KEY (document_id)
);


CREATE TABLE DocumentHistory(
    id INT NOT NULL IDENTITY,
	document_id INT NOT NULL,
    ps INT NOT NULL,
    operation INT NOT NULL,
    amount REAL NOT NULL,
    currency INT NOT NULL,
    status INT NOT NULL,
    date_created DATETIME NOT NULL,
    date_modified DATETIME NOT NULL,
    transaction VARCHAR(255),
    employee VARCHAR(50),
    bettor VARCHAR(50),
    cause VARCHAR(50),
    register_id INT,
    message TEXT,
    CONSTRAINT pk_document_histroty PRIMARY KEY (id)
);

-- *** FOREIGN KEYS ***
ALTER TABLE BettorLedgerEntry ADD CONSTRAINT fk_GamblerLedger FOREIGN KEY (bettor) REFERENCES bet.Person(code);
ALTER TABLE BettorLedgerEntry ADD CONSTRAINT fk_GamblerLedger_0 FOREIGN KEY (related_booking) REFERENCES bet.Booking(booking_no);
ALTER TABLE BettorLedgerEntry ADD CONSTRAINT fk_GamblerLedgerEntry FOREIGN KEY (entry_type) REFERENCES bet.BettorLEntryType(code);
ALTER TABLE BettorLedgerEntry ADD CONSTRAINT fk_BettorLedgerEntry FOREIGN KEY (payment_method) REFERENCES bet.PaymentMethod(code);
ALTER TABLE Booking ADD CONSTRAINT fk_Bet_0 FOREIGN KEY (bettor) REFERENCES bet.Person(code);
ALTER TABLE Booking ADD CONSTRAINT fk_Bet_1 FOREIGN KEY (point) REFERENCES bet.Point(code);
ALTER TABLE BookingOutcomes ADD CONSTRAINT fk_BookingOutcomesBooking FOREIGN KEY (booking_no) REFERENCES bet.Booking(booking_no);
ALTER TABLE BookingOutcomes ADD CONSTRAINT fk_BookingOutcomesEvent FOREIGN KEY (event) REFERENCES bet.Event(id);
ALTER TABLE BookingOutcomes ADD CONSTRAINT fk_BookingOutcomesOutcome FOREIGN KEY (outcome) REFERENCES bet.Outcome(id);
ALTER TABLE Category ADD CONSTRAINT fk_Category FOREIGN KEY (sport) REFERENCES bet.Sport(id);
ALTER TABLE OutcomeOdds ADD CONSTRAINT fk_OutcomeOddsOutcome FOREIGN KEY (outcome) REFERENCES bet.Outcome(id);
ALTER TABLE OutcomeOdds ADD CONSTRAINT fk_OutcomeOddsEvent FOREIGN KEY (event) REFERENCES bet.Event(id);
ALTER TABLE OutcomeOdds ADD CONSTRAINT fk_OutcomeOddsgAmeState FOREIGN KEY (game_state) REFERENCES bet.GameState(id);
ALTER TABLE Employee ADD CONSTRAINT fk_Employee FOREIGN KEY (code) REFERENCES bet.Person(code);
ALTER TABLE Event ADD CONSTRAINT fk_Event FOREIGN KEY (sport_id) REFERENCES bet.Sport(id);
ALTER TABLE GameState ADD CONSTRAINT fk_GameStateEvent FOREIGN KEY (event) REFERENCES bet.Event(id);
ALTER TABLE GeneralLedgerEntry ADD CONSTRAINT fk_Ledger FOREIGN KEY (point) REFERENCES bet.Point(code);
ALTER TABLE GeneralLedgerEntry ADD CONSTRAINT fk_Ledger_0 FOREIGN KEY (entry_type) REFERENCES bet.GenLEntryType(code);
ALTER TABLE GeneralLedgerEntry ADD CONSTRAINT fk_GeneralLedger_0 FOREIGN KEY (related_booking) REFERENCES bet.Booking(booking_no);
ALTER TABLE GeneralLedgerEntry ADD CONSTRAINT fk_GeneralLedgerEntry FOREIGN KEY (account) REFERENCES bet.GLAccount(code);
ALTER TABLE GeneralLedgerEntry ADD CONSTRAINT fk_GeneralLedgerEntry_0 FOREIGN KEY (payment_method) REFERENCES bet.PaymentMethod(code);
ALTER TABLE OutcomeOddsHistory ADD CONSTRAINT fk_OutcomeOddsHistory_outcome FOREIGN KEY (outcome) REFERENCES bet.Outcome(id);
ALTER TABLE OutputMessage ADD CONSTRAINT fk_Message FOREIGN KEY (person) REFERENCES bet.Person(code);
ALTER TABLE Person ADD CONSTRAINT fk_Person_country FOREIGN KEY (country) REFERENCES bet.Country(id);
ALTER TABLE Point ADD CONSTRAINT fk_Point FOREIGN KEY (city) REFERENCES bet.City(code);
ALTER TABLE WithdrawalRequest ADD CONSTRAINT fk_WithdrawalRequest FOREIGN KEY (bettor) REFERENCES bet.Person(code);
ALTER TABLE WithdrawalRequestHistory ADD CONSTRAINT fk_WithdrawalRequestHistory FOREIGN KEY (employee) REFERENCES bet.Employee(code);
ALTER TABLE WithdrawalRequestHistory ADD CONSTRAINT fk_WithdrawalRequestHistory_0 FOREIGN KEY (request) REFERENCES bet.WithdrawalRequest(entry_no);
ALTER TABLE WithdrawalRequest ADD CONSTRAINT fk_WithdrawalPay FOREIGN KEY (payment_method) REFERENCES bet.PaymentMethod(code);
ALTER TABLE PointEmployeeMapping ADD CONSTRAINT fk_PEMtoPoint FOREIGN KEY (point_code) REFERENCES bet.Point(code);
ALTER TABLE PointEmployeeMapping ADD CONSTRAINT fk_PEMtoEmp FOREIGN KEY (employee_code) REFERENCES bet.Employee(code);
ALTER TABLE BettorAccount ADD CONSTRAINT fk_person_account FOREIGN KEY (code) REFERENCES bet.Person(code);

ALTER TABLE BettorLedgerEntry ADD CONSTRAINT fk_ble_register FOREIGN KEY (register_id) REFERENCES bet.Register(id);
ALTER TABLE GeneralLedgerEntry ADD CONSTRAINT fk_gle_register FOREIGN KEY (register_id) REFERENCES bet.Register(id);

ALTER TABLE ConfirmationToken ADD CONSTRAINT fk_confirmation_token_bettor FOREIGN KEY (bettor) REFERENCES bet.Person(code);

ALTER TABLE Person ADD CONSTRAINT fk_PersonCurrency FOREIGN KEY (currency) REFERENCES bet.Currency(numeric_code);
ALTER TABLE Booking ADD CONSTRAINT fk_BookingCurrency FOREIGN KEY (currency) REFERENCES bet.Currency(numeric_code);
ALTER TABLE WithdrawalRequest ADD CONSTRAINT fk_WthdRequestCurrency FOREIGN KEY (currency) REFERENCES bet.Currency(numeric_code);
ALTER TABLE BettorLedgerEntry ADD CONSTRAINT fk_BLECurrency FOREIGN KEY (currency) REFERENCES bet.Currency(numeric_code);
ALTER TABLE GeneralLedgerEntry ADD CONSTRAINT fk_GLECurrency FOREIGN KEY (currency) REFERENCES bet.Currency(numeric_code);
ALTER TABLE DocumentHistory ADD CONSTRAINT fk_document_history FOREIGN KEY (document_id) REFERENCES bet.Document(document_id);
-- *** INDICES ***
CREATE INDEX idx_GamblerLedger ON BettorLedgerEntry(bettor);
CREATE INDEX idx_GamblerLedger_0 ON BettorLedgerEntry(related_booking);
CREATE INDEX idx_GamblerLedgerEntry ON BettorLedgerEntry(entry_type);
CREATE INDEX idx_BettorLedgerEntry ON BettorLedgerEntry(payment_method);
CREATE INDEX idx_Bet_0 ON Booking(bettor);
CREATE INDEX idx_Bet_1 ON Booking(point);
CREATE INDEX idx_bet_status ON Booking(status);
CREATE INDEX idx_BookingOutcomes ON BookingOutcomes(booking_no);
CREATE INDEX idx_BookingOutcomes_0 ON BookingOutcomes(outcome);
CREATE INDEX idx_BookingOutcomesEvent ON BookingOutcomes(event);
CREATE INDEX Pk_City_0 ON City(name);
CREATE INDEX idx_Category ON Category(sport);
CREATE INDEX idx_Outcome ON Outcome(sport_id, code);
CREATE INDEX idx_Event ON Event(category);
CREATE INDEX idx_Event2 ON Event(status);
CREATE INDEX idx_IncompleteEventsGridSort ON Event(id, dt);
CREATE INDEX Pk_Ledger_0 ON GeneralLedgerEntry(point);
CREATE INDEX idx_Ledger ON GeneralLedgerEntry(entry_type);
CREATE INDEX idx_GeneralLedger_0 ON GeneralLedgerEntry(related_booking);
CREATE INDEX idx_GeneralLedgerEntry ON GeneralLedgerEntry(account);
CREATE INDEX idx_GeneralLedgerEntry_0 ON GeneralLedgerEntry(payment_method);
--CREATE INDEX idx_Outcome ON Outcome(event);
CREATE INDEX idx_OutcomeOddsHistory ON OutcomeOddsHistory(outcome);
CREATE INDEX idx_Message ON OutputMessage(person);
CREATE INDEX idx_Point ON Point(city);
CREATE INDEX idx_WithdrawalRequest ON WithdrawalRequest(bettor);
CREATE INDEX idx_WithdrawalRequestHistory ON WithdrawalRequestHistory(request);
CREATE INDEX Pk_WithdrawalRequestHistory_0 ON WithdrawalRequestHistory(employee);
CREATE INDEX idx_EmpPoMap ON PointEmployeeMapping(point_code);
-- *** VIEWS *** --
--CREATE VIEW BettorInfo AS
--	SELECT
--		betta.code as code, betta.limit as limit, pers.first_name as first_name,
--		pers.second_name as second_name, pers.family_name as family_name, pers.trust_level as trust_level,
--		pers.passport_no as passport_no, pers.born_date as born_date, pers.sex as sex, betta.kind as kind,
--		betta.is_blocked as is_blocked
--	FROM bet.Bettor as betta
--	INNER JOIN bet.Person as pers
--		ON pers.code = betta.code;

/*Исходы, на которые были сделаны ставки.*/
CREATE VIEW BettedOutcomes AS
	SELECT DISTINCT event, outcome FROM bet.BookingOutcomes;

CREATE VIEW IncompleteBettedEvents AS
    SELECT e.id, e.sport_id, e.dt, e.status
    FROM bet.Event AS e
        LEFT JOIN bet.BookingOutcomes AS bo ON bo.event = e.id
        LEFT JOIN bet.Booking AS b ON b.booking_no = bo.booking_no AND b.status = 0
    WHERE e.blocked = True AND e.status IN (0, 1) AND b.booking_no IS NULL;

CREATE VIEW BettorAccountView AS
    SELECT
        ba.code AS bettor_code,
        ba.balance AS bettor_balance,
        p.currency
    FROM bet.BettorAccount AS ba
        LEFT JOIN bet.Person AS p ON ba.code = p.code;

CREATE MATERIALIZED VIEW TurnoverBalanceSheet AS
    SELECT account AS account_code, SUM(amount) AS balance, currency AS currency_code
    FROM bet.GeneralLedgerEntry
    GROUP BY account_code, currency_code;
