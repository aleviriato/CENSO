CREATE OR REPLACE PROCEDURE SP_REGISTRA_LOG_BD (i_nom_evento in  varchar2,
                                                              i_des_chave  in  varchar2,
                                                              i_des_msg    in  varchar2) AS
    PRAGMA AUTONOMOUS_TRANSACTION;

BEGIN

    INSERT INTO RECENSEAMENTO.TB_LOG_BD
            (dat_ocor,
             nom_evento,
             des_chave,
             des_msg)
        VALUES
            (sysdate,
             substr(i_nom_evento, 1, 150),
             substr(i_des_chave, 1, 300),
             substr(i_des_msg, 1, 4000));
    commit;

EXCEPTION
    WHEN OTHERS THEN
        NULL;
END SP_REGISTRA_LOG_BD;
/

