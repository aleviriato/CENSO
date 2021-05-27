CREATE OR REPLACE TRIGGER TB_LOG_BD_BEIN_TGL
    BEFORE INSERT ON RECENSEAMENTO.TB_LOG_BD 
    FOR EACH ROW

BEGIN
    SELECT recenseamento.seq_og_bd.NEXTVAL
    INTO   :new.id
    FROM   dual;
EXCEPTION
  WHEN OTHERS THEN
    RAISE_APPLICATION_ERROR(-20101,
                            substr('(TB_LOG_BD_BEIN_TGL) Erro ao inserir em TB_LOG_BD: ' || sqlerrm, 1, 2048));
END TB_LOG_BD_BEIN_TGL;
/

