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
  $BODY$ LANGUAGE plpgsql VOLATILE COST 100;;



  --MATERIALIZED VIEW INSERT FUNCTION
  CREATE OR REPLACE FUNCTION celesta.materialized_view_insert() RETURNS trigger AS $BODY$
        DECLARE
          mv        varchar;
          mvGrouByCols varchar;
          tableGroupByCols varchar;
          mvAllCols varchar;
          selectStmt varchar;
        BEGIN
          mv := TG_ARGV[0];
          mvGrouByCols := TG_ARGV[1];
          tableGroupByCols := TG_ARGV[2];
          mvAllCols := TG_ARGV[3];
          selectStmt := TG_ARGV[4];

          LOCK TABLE ONLY mv IN ROW EXCLUSIVE MODE;

          EXECUTE 'DELETE FROM ' || mv || ' WHERE (' || mvGrouByCols || ') IN (select ' || tableGroupByCols || ')' USING NEW;
          EXECUTE 'INSERT INTO ' || mv  || '(' || format('%s', mvAllCols) || ') ' || selectStmt USING NEW;

          RETURN NEW;
        END;
  $BODY$LANGUAGE plpgsql VOLATILE COST 100;



  --MATERIALIZED VIEW UPDATE FUNCTION
  CREATE OR REPLACE FUNCTION celesta.materialized_view_update() RETURNS trigger AS $BODY$
        DECLARE
          mv        varchar;
          mvGrouByCols varchar;
          tableGroupByCols varchar;
          mvAllCols varchar;
          selectStmt varchar;
        BEGIN
          mv := TG_ARGV[0];
          mvGrouByCols := TG_ARGV[1];
          tableGroupByCols := TG_ARGV[2];
          mvAllCols := TG_ARGV[3];
          selectStmt := TG_ARGV[4];

          LOCK TABLE ONLY mv IN ROW EXCLUSIVE MODE;


          --PROCESS OLD
          EXECUTE 'DELETE FROM ' || mv || ' WHERE (' || mvGrouByCols || ') IN (select ' || tableGroupByCols || ')' USING OLD;
          EXECUTE 'INSERT INTO ' || mv  || '(' || format('%s', mvAllCols) || ') ' || selectStmt USING OLD;

          --PROCESS NEW
          EXECUTE 'DELETE FROM ' || mv || ' WHERE (' || mvGrouByCols || ') IN (select ' || tableGroupByCols || ')' USING NEW;
          EXECUTE 'INSERT INTO ' || mv  || '(' || format('%s', mvAllCols) || ') ' || selectStmt USING NEW;

          RETURN NEW;
        END;
  $BODY$LANGUAGE plpgsql VOLATILE COST 100;


  --MATERIALIZED VIEW INSERT FUNCTION
  CREATE OR REPLACE FUNCTION celesta.materialized_view_delete() RETURNS trigger AS $BODY$
        DECLARE
          mv        varchar;
          mvGrouByCols varchar;
          tableGroupByCols varchar;
          mvAllCols varchar;
          selectStmt varchar;
        BEGIN
          mv := TG_ARGV[0];
          mvGrouByCols := TG_ARGV[1];
          tableGroupByCols := TG_ARGV[2];
          mvAllCols := TG_ARGV[3];
          selectStmt := TG_ARGV[4];

          LOCK TABLE ONLY mv IN ROW EXCLUSIVE MODE;

          EXECUTE 'DELETE FROM ' || mv || ' WHERE (' || mvGrouByCols || ') IN (select ' || tableGroupByCols || ')' USING OLD;
          EXECUTE 'INSERT INTO ' || mv  || '(' || format('%s', mvAllCols) || ') ' || selectStmt USING OLD;

          RETURN NEW;
        END;
  $BODY$LANGUAGE plpgsql VOLATILE COST 100;