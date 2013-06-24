create grain celesta version '1.00';

create table grains(
  id nvarchar(16) not null primary key, --префикс (код) гранулы
  version  nvarchar(max) not null, -- version tag гранулы
  length int not null, -- длина creation-скрипта гранулы  в байтах (составляющая часть контрольной суммы)
  checksum int not null, --CRC32 creation-скрипта гранулы (составляющая часть контрольной суммы)
  state int not null default 3, -- статус гранулы — см. далее
  lastmodified datetime not null default getdate(), -- дата и время последнего обновления статуса гранулы
  message nvarchar(max) not null default '' -- комментарий (например, сообщение об ошибке при последнем неудавшемся автообновлении)
);