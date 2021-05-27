CREATE OR REPLACE FORCE VIEW VW_RESUMO_CENSO_ATIVO_BKP_2506 AS
SELECT drive.etapa ,
       drive.PROCESSO ,
       drive.TOTAL,
       drive.percentual_conclusao,
       drive.percentual_remanescente
  FROM (
         SELECT 1 ETAPA,
         'FINALIZOU PRE-CADASTRO ONLINE' PROCESSO,
          COUNT(1) TOTAL
        ,round(round(100 * COUNT(1),2)/((select count(*)
                                             from TB_USUARIO A1
                                            WHERE EXISTS (SELECT 1 FROM TB_USUARIO_PERFIL UP WHERE UP.USUARIO_ID = A1.ID AND UP.PERFIL_ID = 2)
                                              AND EXISTS (SELECT 1 FROM TB_PESSOA_FISICA PF WHERE PF.ID = A1.PESSOA_FISICA_ID AND NVL(PF.FLG_EXCLUIDO,'N') <> 'S') )),2) percentual_conclusao
         ,100-(round(round(100 * COUNT(1),2)/((select count(*)
                                                  from TB_USUARIO A1
                                                 WHERE EXISTS (SELECT 1 FROM TB_USUARIO_PERFIL UP WHERE UP.USUARIO_ID = A1.ID AND UP.PERFIL_ID = 2)
                                                   AND EXISTS (SELECT 1 FROM TB_PESSOA_FISICA PF WHERE PF.ID = A1.PESSOA_FISICA_ID AND NVL(PF.FLG_EXCLUIDO,'N') <> 'S') )),2)) percentual_remanescente
         FROM TB_USUARIO A
         WHERE A.FLAG_FINAL_PRE_CAD = 1
           AND to_char(A.dat_final_pre_cad,'dd/mm/yyyy') >= to_date('17/12/2018','dd/mm/yyyy')
           AND EXISTS (SELECT 1 FROM TB_USUARIO_PERFIL UP WHERE UP.USUARIO_ID = A.ID AND UP.PERFIL_ID = 2)
           AND EXISTS (SELECT 1 FROM TB_PESSOA_FISICA PF WHERE PF.ID = A.PESSOA_FISICA_ID AND NVL(PF.FLG_EXCLUIDO,'N') <> 'S')
         group by 1, 'FINALIZOU PRE-CADASTRO ONLINE'
         UNION
         SELECT 2 ETAPA,
         'Acessou o Sistema' PROCESSO,
          COUNT(1) TOTAL
          ,round(round(100 * COUNT(1),2)/(select count(*) from TB_USUARIO A WHERE EXISTS (SELECT 1 FROM TB_PESSOA_FISICA PF WHERE PF.ID = A.PESSOA_FISICA_ID AND NVL(PF.FLG_EXCLUIDO,'N') <> 'S')),2)       percentual_conclusao
          ,100-(round(round(100 * COUNT(1),2)/((select count(*) from TB_USUARIO A WHERE EXISTS (SELECT 1 FROM TB_PESSOA_FISICA PF WHERE PF.ID = A.PESSOA_FISICA_ID AND NVL(PF.FLG_EXCLUIDO,'N') <> 'S'))),2))      percentual_remanescente
          FROM TB_USUARIO A
         WHERE EXISTS (SELECT 1 FROM TB_LOG LO WHERE LO.USUARIO_ID = A.ID)
           AND A.ACTIVE = 1
           AND EXISTS (SELECT 1 FROM TB_USUARIO_PERFIL UP WHERE UP.USUARIO_ID = A.ID AND UP.PERFIL_ID = 2)
           AND EXISTS (SELECT 1 FROM TB_PESSOA_FISICA PF WHERE PF.ID = A.PESSOA_FISICA_ID AND NVL(PF.FLG_EXCLUIDO,'N') <> 'S')
         group by 2, 'Acessou o Sistema'

        UNION

          SELECT 3 ETAPA,
          'Alterou Dados Pessoais' PROCESSO,
          COUNT(1) TOTAL
          ,round(round(100 * COUNT(1),2)/((select count(*)      from TB_PESSOA_FISICA WHERE NVL(FLG_EXCLUIDO,'N')<>'S')),2)       percentual_conclusao
          ,100-(round(round(100 * COUNT(1),2)/((select count(*) from TB_PESSOA_FISICA WHERE NVL(FLG_EXCLUIDO,'N')<>'S')),2))      percentual_remanescente
            FROM TB_PESSOA_FISICA
           WHERE FLAG_RECADASTRAMENTO = 1 AND NVL(FLG_EXCLUIDO,'N') <> 'S'
           group by 3, 'Alterou Dados Pessoais'

        UNION
            SELECT 4 ETAPA,
           'INCLUIU / ALTEROU ENDERECO' PROCESSO,
            COUNT(1) TOTAL
            ,round(round(100 * COUNT(1),2)/(select count(*)      from TB_USUARIO A WHERE EXISTS (SELECT 1 FROM TB_PESSOA_FISICA PF WHERE PF.ID = A.PESSOA_FISICA_ID AND NVL(PF.FLG_EXCLUIDO,'N')<>'S')),2)       percentual_conclusao
            ,100-(round(round(100 * COUNT(1),2)/(select count(*) from TB_USUARIO A WHERE EXISTS (SELECT 1 FROM TB_PESSOA_FISICA PF WHERE PF.ID = A.PESSOA_FISICA_ID AND NVL(PF.FLG_EXCLUIDO,'N')<>'S')),2))      percentual_remanescente
            FROM TB_USUARIO A
           WHERE EXISTS (SELECT 1 FROM TB_END_PESSOA_FISICA EP, TB_PESSOA_FISICA PF WHERE EP.COD_IDE_CLI = PF.COD_IDE_CLI AND EP.FLAG_RECADASTRAMENTO = 1 AND PF.ID = A.PESSOA_FISICA_ID )
             AND EXISTS (SELECT 1 FROM TB_PESSOA_FISICA PF WHERE PF.ID = A.PESSOA_FISICA_ID AND NVL(PF.FLG_EXCLUIDO,'N') <> 'S')
           group by 4, 'INCLUIU / ALTEROU ENDERECO'
        UNION
        SELECT 5 ETAPA
             , 'Incluiu Dependente' PROCESSO
             , COUNT(1) TOTAL
             , round(round(100 * COUNT(1),2)/((select count(*)      from TB_USUARIO A1 WHERE EXISTS (SELECT 1 FROM TB_BENEFICIARIO   BE, TB_PESSOA_FISICA PF WHERE BE.COD_IDE_CLI = PF.COD_IDE_CLI AND PF.ID = A1.PESSOA_FISICA_ID AND BE.COD_TIPO_BENEFICIO <> '1' AND NVL(PF.FLG_EXCLUIDO,'N')<> 'S'))),2)  percentual_conclusao
             , 100-(round(round(100 * COUNT(1),2)/((select count(*) from TB_USUARIO A2 WHERE EXISTS (SELECT 1 FROM TB_BENEFICIARIO   BE, TB_PESSOA_FISICA PF WHERE BE.COD_IDE_CLI = PF.COD_IDE_CLI AND PF.ID = A2.PESSOA_FISICA_ID AND BE.COD_TIPO_BENEFICIO <> '1' AND NVL(PF.FLG_EXCLUIDO,'N')<> 'S'))),2)) percentual_remanescente
          FROM TB_USUARIO A
         WHERE EXISTS (SELECT 1 FROM TB_USUARIO_PERFIL UP WHERE UP.USUARIO_ID = A.ID AND UP.PERFIL_ID = 2)
           AND EXISTS (SELECT 1 FROM TB_DEPENDENTE     DP, TB_PESSOA_FISICA PF WHERE DP.COD_IDE_CLI = PF.COD_IDE_CLI AND DP.FLAG_RECADASTRAMENTO = 1 AND PF.ID = A.PESSOA_FISICA_ID )
           AND EXISTS (SELECT 1 FROM TB_BENEFICIARIO   BE, TB_PESSOA_FISICA PF WHERE BE.COD_IDE_CLI = PF.COD_IDE_CLI AND PF.ID = A.PESSOA_FISICA_ID AND BE.COD_TIPO_BENEFICIO <> '1')
           AND EXISTS (SELECT 1 FROM TB_PESSOA_FISICA PF WHERE PF.ID = A.PESSOA_FISICA_ID AND NVL(PF.FLG_EXCLUIDO,'N') <> 'S')
            group by 5, 'Incluiu Dependente'
        UNION
        SELECT 6 ETAPA
             , 'Incluiu Representante Legal' PROCESSO
             , COUNT(1) TOTAL
             , round(round(100 * COUNT(1),2)/(select count(*) from TB_USUARIO A1 WHERE EXISTS (SELECT 1 FROM TB_USUARIO_PERFIL UP WHERE UP.USUARIO_ID = A1.ID AND UP.PERFIL_ID = 2) AND EXISTS (SELECT 1 FROM TB_REPRESENTANTE_LEGAL BE, TB_PESSOA_FISICA PF WHERE BE.COD_IDE_CLI = PF.COD_IDE_CLI AND PF.ID = A1.PESSOA_FISICA_ID AND EXISTS (SELECT 1 FROM TB_PESSOA_FISICA PF WHERE PF.ID = A1.PESSOA_FISICA_ID AND NVL(PF.FLG_EXCLUIDO,'N')<>'S'))),2)  percentual_conclusao
             , 0 percentual_remanescente
          FROM TB_USUARIO A
         WHERE EXISTS (SELECT 1 FROM TB_USUARIO_PERFIL UP WHERE UP.USUARIO_ID = A.ID AND UP.PERFIL_ID = 2)
           AND EXISTS (SELECT 1 FROM TB_REPRESENTANTE_LEGAL BE, TB_PESSOA_FISICA PF WHERE BE.COD_IDE_CLI = PF.COD_IDE_CLI AND BE.FLAG_RECADASTRAMENTO = 1 AND PF.ID = A.PESSOA_FISICA_ID)
           AND EXISTS (SELECT 1 FROM TB_PESSOA_FISICA PF WHERE PF.ID = A.PESSOA_FISICA_ID AND NVL(PF.FLG_EXCLUIDO,'N') <> 'S')
            group by 6, 'Incluiu Representante Legal', 0
) DRIVE
ORDER BY DRIVE.ETAPA;

