CREATE GRAIN logs VERSION '1.2';

/**This is Celesta doc.
This is second line of Celestadoc.*/
CREATE TABLE SystemLog(
	id INT IDENTITY NOT NULL,
	level INT NOT NULL,
	sender_type INT NOT NULL,
	sender_id VARCHAR(50) NOT NULL,
	dt DATETIME NOT NULL,
	event_id INT,
	target_type INT,
	target_id VARCHAR(50),
	result INT NOT NULL DEFAULT 0,
	message VARCHAR(500),
	
	CONSTRAINT Pk_SystemLog PRIMARY KEY (id)
) WITH NO VERSION CHECK;


/**This is Celesta doc.
This is second line of Celestadoc.*/
CREATE TABLE BettorLog(
	id INT IDENTITY NOT NULL,
	level INT NOT NULL,
	sender_type INT NOT NULL,
	/**{caption: 'SID сотрудника'}*/
	sender_id VARCHAR(50) NOT NULL,
	/**{caption: 'Время изменения'}*/
	dt DATETIME NOT NULL,
	event_id INT,
	target_type INT,
	/**{caption: 'ID пользователя'}*/
	target_id VARCHAR(50),
	result INT NOT NULL DEFAULT 0,
	/**{caption: 'Сообщение'}*/
	message TEXT,
	
	CONSTRAINT Pk_BettorLog PRIMARY KEY (id)
) WITH NO VERSION CHECK;

CREATE TABLE Notifications(
	id INT IDENTITY NOT NULL,
	level INT NOT NULL,
	sender_type INT,
	sender_id VARCHAR(50),
	dt DATETIME NOT NULL,
	event_id INT,
	target_type INT,
	target_id VARCHAR(50),
	result INT,
	message VARCHAR(500),
	CONSTRAINT Pk_Notifications PRIMARY KEY (id)
);
