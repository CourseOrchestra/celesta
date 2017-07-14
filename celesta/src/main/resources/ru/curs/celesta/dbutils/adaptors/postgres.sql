  /*
    Этот файл содержит инструкции для создания системных объектов celesta для POSTGRESSQL
   */



   --RECVERSION CHECK FUNCTION
  CREATE OR REPLACE FUNCTION celesta.recversion_check() RETURNS trigger AS $BODY$ BEGIN
    IF (OLD.recversion = NEW.recversion) THEN
      NEW.recversion = NEW.recversion + 1;
    ELSE
      RAISE EXCEPTION 'record version check failure';
    END IF
    RETURN NEW; 
    END;
  $BODY$ LANGUAGE plpgsql VOLATILE COST 100;