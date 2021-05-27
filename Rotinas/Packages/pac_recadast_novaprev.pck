CREATE OR REPLACE PACKAGE PAC_RECADAST_NOVAPREV is
--
    
   type typ_rec_ben_pendente is record
   (COD               tb_pessoa_fisica.cod_ide_cli%type,
    ID                tb_pessoa_fisica.id%type,   
    NUM_PRONTUARIO    tb_beneficiario.num_prontuario%type,
    NOM_BENEFICIARIO  tb_pessoa_fisica.nom_pessoa_fisica%type,
    NUM_CPF           tb_pessoa_fisica.num_cpf%type,  
    TIPO_BENEFICIO    tb_dominio_detalhe.name%type,  
    DAT_CONCESSAO     tb_beneficiario.dat_concessao%type,    
    TIP_LOGRADOURO    tb_end_tipo_logradouro.nom_tipo%type,
    LOGRADOURO        tb_end_logradouro.nom_logradouro%type,    
    NUMERO            tb_end_pessoa_fisica.num_numero%type,    
    BAIRRO            tb_end_bairro.nom_bairro%type,
    MUNICIPIO         tb_end_localidade.nom_localidade%type,
    CEP               tb_end_pessoa_fisica.num_cep%type,
    ESTADO            tb_dominio_detalhe.Cod_Domain_Detail%type,
    TELEFONE          tb_pessoa_fisica.num_tel_fone1%type,
    DAT_NASC          tb_pessoa_fisica.dat_nasc%type,
    FLG_SIT_RECAD     char,
    FLG_COMP_DIA_AGEN char,                
    FLG_ATEND_INI_NAO_FINALIZADO char,
    FLG_AGENDOU      char,
    DES_SIT_RECAD     tb_dominio_detalhe.name%type);
    
    type typ_tab_ben_pendente is table of typ_rec_ben_pendente;

    v_role_user_id          recenseamento.tb_perfil.id%type; 
    
    FUNCTION FNC_IDENT_ID_BARRO_MUN(I_COD_UF                 IN TB_END_LOGRADOURO.COD_UF%TYPE,
                                    I_NUM_CEP                IN TB_END_PESSOA_FISICA.NUM_CEP%TYPE,
                                    I_NOM_BAIRRO_CARREGADO   IN VARCHAR2,
                                    O_ID_MUNICIPIO           OUT NUMBER,
                                    O_ID_BAIRRO              OUT NUMBER
                                   ) RETURN CHAR; --S OU N PARA SE LOCALOIZOU OU NAO

    PROCEDURE SP_CARGA_MASSIVA_RECADAST (i_mes_ini number, 
                                         i_mes_fim number);
     PROCEDURE SP_CARGA_ENDERECO(I_COD_INS     VARCHAR2,
                                I_COD_IDE_CLI VARCHAR2);

    PROCEDURE SP_CARGA_DADOS_RECADAST (i_cod_ins     in     number,
                                       i_cod_ide_cli in     varchar2,
                                       o_msg         in out varchar2);

    PROCEDURE SP_REGISTRA_ACESSO_USUARIO (i_pessoa_fisica_id in recenseamento.tb_usuario.pessoa_fisica_id%type,
                                          i_username         in recenseamento.tb_usuario.username%type,
                                          o_msg              in out varchar2);

    FUNCTION FNC_PERM_CARGA_PESSOA_RECAD (i_cod_ins     in number,
                                          i_cod_ide_cli in varchar2) RETURN VARCHAR2;
                                          
    -- Rotina que Atualiza a data de obito do recenseamento a partir da novaprev.    
    PROCEDURE sp_atu_pf_dat_obito(i_cod_ins     in tb_pessoa_fisica.cod_ins%type,
                                  i_cod_ide_cli in tb_pessoa_fisica.cod_ide_cli%type,
                                  o_cod_erro   out number,
                                  o_msg_erro   out varchar2);    
                                  
    -- Rotina que retorna dados do relatorio de Beneficiarios pendentes de Recadastramento.
    FUNCTION fnc_ret_rel_ben_pendente (i_nom_pessoa_fisica in varchar2,
                                       i_num_cpf           in varchar2,
                                       i_num_matricula     in varchar2, 
                                       i_mes_aniversario   in varchar2) return typ_tab_ben_pendente pipelined;

    -- Rotina que remove  todos os agendamentos de uma pessoa fisica    
    PROCEDURE sp_remove_agendamento_pf(i_id_pf in tb_pessoa_fisica.id%type,                                                                                                            
                                       o_cod_erro out number,
                                       o_msg_erro out varchar2);
                                       
    PROCEDURE sp_atu_pf_ben_cessado(i_cod_ins     in tb_pessoa_fisica.cod_ins%type,
                                    i_cod_ide_cli in tb_pessoa_fisica.cod_ide_cli%type,
                                    o_cod_erro   out number,
                                    o_msg_erro   out varchar2);     
                                    
    PROCEDURE sp_atu_pf_ben_2019(i_cod_ins     in tb_pessoa_fisica.cod_ins%type,
                                 i_cod_ide_cli in tb_pessoa_fisica.cod_ide_cli%type,
                                 o_cod_erro   out number,
                                 o_msg_erro   out varchar2);   
                                 

    procedure sp_login_atu_cadastro(i_cod_ins     in tb_pessoa_Fisica.cod_ins%type,
                                    i_cpf_login   in tb_pessoa_Fisica.num_cpf%type,
                                    i_cod_ide_cli in tb_pessoa_Fisica.cod_ide_cli%type,                                   
                                    o_msg_erro    out varchar2);                                                                  

END PAC_RECADAST_NOVAPREV;
/

CREATE OR REPLACE PACKAGE BODY PAC_RECADAST_NOVAPREV is
    v_char char;
    FUNCTION FNC_IDENT_ID_BARRO_MUN(I_COD_UF                 IN TB_END_LOGRADOURO.COD_UF%TYPE,
                                    I_NUM_CEP                IN TB_END_PESSOA_FISICA.NUM_CEP%TYPE,
                                    I_NOM_BAIRRO_CARREGADO   IN VARCHAR2,
                                    O_ID_MUNICIPIO           OUT NUMBER,
                                    O_ID_BAIRRO              OUT NUMBER
                                   ) RETURN CHAR --S OU N PARA SE LOCALOIZOU OU NAO
    AS
      v_cod_municipio number;
      v_cod_bairro    number;
    BEGIN 
         -- Verifica municipio e bairro pelo CEP
        begin 
          select elog.id_localidade,  elog.id_bairro_ini
            into v_cod_municipio, v_cod_bairro
            from tb_end_logradouro elog
           where elog.cod_uf = I_COD_UF
             and elog.cep = I_NUM_CEP;
        exception 
          when no_data_found then 
            v_cod_municipio := null;
            v_cod_bairro := null;  
        end;             
        --
        --
        if v_cod_municipio is null or v_cod_bairro is null then 
          -- Tenta localizar pela localidade (CEP UNICO)
          begin 
            select eloc.id 
              into v_cod_municipio
              from tb_end_localidade eloc
             where eloc.cod_uf = I_COD_UF
               and eloc.cep = I_NUM_CEP;
          exception 
             when no_data_found then 
               v_cod_municipio := null;
               v_cod_bairro := null;  
           end;         
           --
           -- se achou municipio tenta encontrar bairro pelo nome 
           if v_cod_municipio is not null and I_NOM_BAIRRO_CARREGADO is not null then 
             begin 
               select ba.id
                 into v_cod_bairro
                 from tb_end_bairro ba
                where ba.cod_uf =I_COD_UF
                  and ba.id_localidade =  v_cod_municipio
                  and upper(trim(FNC_TIRA_ACENTO(ba.nom_bairro))) = upper(trim(FNC_TIRA_ACENTO(I_NOM_BAIRRO_CARREGADO)));
             exception 
               when no_data_found then 
                  v_cod_bairro := null;
             end; 
           end if;
           
        end if;

        O_ID_MUNICIPIO := v_cod_municipio;
        O_ID_BAIRRO := v_cod_bairro;

        if v_cod_municipio is null then 
          return 'N';
        else
          return 'S';  
        end if;
    
    END;
    
    -----------------------------------------------------------------------------------------------
    PROCEDURE SP_CARGA_MASSIVA_RECADAST (i_mes_ini number, 
                                         i_mes_fim number) IS

        v_msg_carga     varchar2(1024);
        ERRO_PARAMETROS EXCEPTION;
        ERRO_CARGA      EXCEPTION;
        qtd_processado  number := 0;
        v_lista_mes     varchar2(50) := '#';
        v_cur_mes       number := 0;

    BEGIN

        if ((nvl(i_mes_ini, 0) not between 1 and 12) or 
            (nvl(i_mes_fim, 0) not between 1 and 12)) then
            raise ERRO_PARAMETROS;
        end if;

        v_cur_mes := i_mes_ini;

        loop
            v_lista_mes := v_lista_mes ||trim(to_char(v_cur_mes, '00'))||'#';
            exit when v_cur_mes = i_mes_fim;
            v_cur_mes := v_cur_mes + 1;
            if (v_cur_mes > 12) then
                v_cur_mes := 1;
            end if;
        end loop;

       

        for pf in (select distinct a.cod_ins, a.cod_ide_cli
                   from user_ipesp.tb_pessoa_fisica a
                       inner join user_ipesp.tb_beneficiario b
                           on     a.cod_ins     = b.cod_ins
                              and a.cod_ide_cli = b.cod_ide_cli_ben
                              and upper(b.flg_status) in ('A', 'S')
                              and nvl(b.dat_fim_ben, (sysdate + 1)) > sysdate
                   where a.dat_nasc is not null
                     and instr(v_lista_mes, to_char(a.dat_nasc, 'MM')) > 0
                     and not exists (select 'x'
                                     from recenseamento.tb_pessoa_fisica c
                                     where c.cod_ins     = a.cod_ins
                                       and c.cod_ide_cli = a.cod_ide_cli)                                  
                   ) loop

            sp_carga_dados_recadast(pf.cod_ins, pf.cod_ide_cli, v_msg_carga);

            --if (v_msg_carga is not null) then
            --    raise ERRO_CARGA;
            --end if;

            qtd_processado := qtd_processado + 1;

            if (qtd_processado > 100) then
                commit;
                qtd_processado := 0;
            end if;

        end loop;

        commit;

    EXCEPTION
        WHEN ERRO_PARAMETROS THEN
            raise_application_error(-20001, 'Os parametros de entrada n?o est?o preenchidos corretamente:'||
                                                ' mes inicial: '||i_mes_ini||', mes final: '||i_mes_fim);

        WHEN ERRO_CARGA THEN
            rollback;
            raise_application_error(-20002, v_msg_carga);

        WHEN OTHERS THEN    
            rollback;
            raise_application_error(-20003, sqlerrm);

    END SP_CARGA_MASSIVA_RECADAST;

    -----------------------------------------------------------------------------------------------
    PROCEDURE SP_CARGA_DEPENDENTE(I_COD_INS     VARCHAR2,
                                  I_COD_IDE_CLI VARCHAR2)
    AS
      V_DEP_ID NUMBER;
      --
      v_cod_est_civ     TB_DEPENDENTE.COD_EST_CIV%type; 
      v_cod_org_emi_rg  TB_DEPENDENTE.COD_ORG_EMI_RG%type; 
      v_cod_pais_nasc   TB_DEPENDENTE.COD_PAIS_NASC%type;
      v_cod_uf_emi_rg   TB_DEPENDENTE.COD_UF_EMI_RG%type;
      v_cod_uf_nasc     TB_DEPENDENTE.COD_UF_NASC%type;
      v_id_pais         TB_DEPENDENTE.PAIS_ID%type;       
      v_cod_uf          TB_DEPENDENTE.COD_UF%type;        
      v_cod_municipio   TB_DEPENDENTE.COD_MUNICIPIO%type;      
      v_cod_bairro      TB_DEPENDENTE.COD_BAIRRO%type;
      --
      v_tipo_Dep        tb_Dependente.Cod_Tipo_Dependente%type;
      
      v_qtd_dep_prev    number := 0;
      v_qtd_dep_econ    number := 0;
      
      V_FLG_RECADASTRAMENTO NUMBER; 
      V_FLG_RECENSEAMENTO NUMBER;
    BEGIN       
      FOR R IN
      (SELECT
        PF.COD_NACIO,            
        PF.COD_EST_CIV, 
        DEP.COD_PARENTESCO, 
        PF.COD_ORG_EMI_RG,      
        PF.COD_PAIS_NASC,                                                        
        PF.COD_RACA,             
        PF.COD_SEXO,   
        PF.COD_UF_EMI_RG,                
        PF.COD_UF_NASC,    
        PF.DAT_EMI_RG,           
        PF.DAT_NASC,             
        PF.DES_EMAIL,                    
        DEP.FLG_SAL_FAM,
        DEP.FLG_DEP_IR,           
        PF.NOM_PESSOA_FISICA,    
        PF.NUM_CARTORIO_CASA,    
        PF.NUM_CARTORIO_NASC,    
        PF.NUM_CPF,              
        PF.NUM_FOLHA_CASA,       
        PF.NUM_FOLHA_NASC,       
        PF.NUM_LIVRO_CASA,       
        PF.NUM_LIVRO_NASC,       
        PF.NUM_RG,               
        PF.NUM_TEL_1,            
        PF.NUM_TEL_2,            
        PF.NUM_TEL_3,            
        PF.TIP_NUM_TEL_1,        
        PF.TIP_NUM_TEL_2,        
        PF.TIP_NUM_TEL_3,        
        PF.FLG_DEF_LABORAIS,     
        END.COD_TIPO_END,      
        END.COD_PAIS,                
        END.NUM_CEP,              
        END.COD_UF,       
        END.COD_MUNICIPIO,      
        user_ipesp.pac_integr_util.fnc_depara_bairro_mun_carga(END.COD_MUNICIPIO) as COD_BAIRRO,           
        END.COD_TIPO_LOGRADOURO,  
        END.NOM_LOGRADOURO,       
        END.NUM_NUMERO,           
        END.DES_COMPLEMENTO,      
        END.DES_PONTO_REFERENCIA,
        PF.DES_NATURAL,
        DEP.COD_IDE_CLI_DEP,
        END.NOM_BAIRRO_CARREGADO
        FROM USER_IPESP.TB_DEPENDENTE DEP 
            JOIN USER_IPESP.TB_PESSOA_FISICA PF 
              ON PF.COD_INS = DEP.COD_INS
              AND PF.COD_IDE_CLI = DEP.COD_IDE_CLI_DEP
            LEFT JOIN USER_IPESP.TB_END_PESSOA_FISICA END
                ON PF.COD_INS = END.COD_INS
                AND PF.COD_IDE_CLI = END.COD_IDE_CLI
        WHERE DEP.COD_IDE_CLI_SERV = I_COD_IDE_CLI          
      )      
      LOOP
        --
        v_cod_pais_nasc  := user_ipesp.pac_integr_util.fnc_ret_depara_valor(R.COD_PAIS_NASC,5,2);
        v_cod_uf_emi_rg  := user_ipesp.pac_integr_util.fnc_ret_depara_valor(R.COD_UF_EMI_RG,10,2);
        v_cod_uf_nasc    := user_ipesp.pac_integr_util.fnc_ret_depara_valor(R.COD_UF_NASC,10,2);
        v_id_pais        := user_ipesp.pac_integr_util.fnc_ret_depara_valor(R.COD_PAIS,8,2);
        v_cod_uf         := user_ipesp.pac_integr_util.fnc_ret_depara_valor(R.COD_UF,10,2);
        v_cod_municipio  := user_ipesp.pac_integr_util.fnc_depara_bairro_mun_carga(R.COD_MUNICIPIO);
        v_cod_bairro     := user_ipesp.pac_integr_util.fnc_depara_bairro_mun_carga(R.COD_BAIRRO);                                                                                                                                                               
        --
        -- --0 Outros
        --10  Conjuge
        --20  Companheiro(a)
        --30  Companheiro Homosexual
        --40  Pai/Mae
        --50  Irmao(a)
        --60  Conjuge separado de fato
        --100 Cônjuge com invalidez/deficiência
        --101 "Ex-cônjuge com invalidez/deficiência        
        --102 Companheiro(a) com invalidez/deficiência
        --103 Ex-companheiro(a)  com invalidez/deficiencia
        --104 Filho(a) com invalidez/deficiencia
        --105 Pai/Mãe com invalidez/deficiência
        --106 Enteado(a) com invalidez/deficiencia
        --107 Menor tutelado(a)  com invalidez/deficiencia
        --11  Ex-conjuge
        --12  Neto(a) (rep. filho pre-morto)
        --13  Menor sob curatela
        --14  Avos
        --15  Ex-Companheiro(a)
        --3 Filho(a)
        --4 Enteado(a)
        --5 Menor sob guarda
        --6 Neto(a)
        --7 Filho(a) Menor de 24 Anos - Universitario
        --8 Menor Tutelado(a)
        --9 Dependente Pensao Judicial
        --90  Padrasto/Madrasta
        --92  Legatario
        --93  Neto(a) menor de 24 anos - Universitario
        --94  Dependente Instituído
        --95  Filho(a) Maior de 25 anos - Universitario
        --96  Neto(a) Maior de 25 anos - Universitario
        --99999 Padrao
        --
        if R.COD_PARENTESCO in ('10','20','30','100', '102','104',3,7) then
           v_tipo_Dep := 1;
           v_qtd_dep_prev := v_qtd_dep_prev+1;
        else 
           v_tipo_Dep := 2;
           v_qtd_dep_econ := v_qtd_dep_econ+1;
        end if; 
        --
        -- Verifica municipio e bairro pelo CEP
        v_char :=  FNC_IDENT_ID_BARRO_MUN(R.COD_UF, 
                                          R.NUM_CEP, 
                                          R.NOM_BAIRRO_CARREGADO, 
                                          v_cod_municipio,
                                          v_cod_bairro
                                          );
        --
        BEGIN 
          SELECT NVL(FLAG_RECADASTRAMENTO,0), NVL(FLAG_RECENSEAMENTO,0)
            INTO V_FLG_RECADASTRAMENTO, V_FLG_RECENSEAMENTO 
            FROM TB_DEPENDENTE DP
           WHERE DP.COD_IDE_CLI =  I_COD_IDE_CLI
             AND DP.COD_IDE_CLI_DEP = R.COD_IDE_CLI_DEP;
        EXCEPTION 
          WHEN NO_DATA_FOUND THEN 
            V_FLG_RECADASTRAMENTO := NULL;
            V_FLG_RECENSEAMENTO   := NULL;
        END;
        --   
        IF V_FLG_RECADASTRAMENTO IS NULL THEN       
          V_DEP_ID := SEQ_EPENDENTE.NEXTVAL;
          --
          INSERT INTO TB_DEPENDENTE 
          (ID,
           COD_CID_NASC,
           COD_EST_CIV,
           COD_GRAU_PARENTESCO,
           COD_IDE_CLI,
           COD_INS,
           COD_NACIO,
           COD_ORG_EMI_RG,
           COD_PAIS_NASC,
           COD_RACA,
           COD_SEXO,
           COD_UF_EMI_RG,
           COD_UF_NASC,
           DAT_CRIACAO,
           DAT_EMI_RG,
           DAT_NASC,
           DAT_RECADASTRAMENTO,
           DAT_RECENSEAMENTO,
           DAT_ULT_ATU,
           EMAIL,
           FLAG_RECADASTRAMENTO,
           FLAG_RECENSEAMENTO,
           FLG_SAL_FAM,
           HAS_DEPENDENTE_IR,
           NOM_PESSOA_FISICA,
           NOM_PRO_CRIACAO,
           NOM_PRO_ULT_ATU,
           NOM_USU_CRIACAO,
           NOM_USU_ULT_ATU,
           NUM_CARTORIO_CASA,
           NUM_CARTORIO_NASC,
           NUM_CPF,
           NUM_FOLHA_CASA,
           NUM_FOLHA_NASC,
           NUM_LIVRO_CASA,
           NUM_LIVRO_NASC,
           NUM_RG,
           NUM_TEL1,
           NUM_TEL2,
           NUM_TEL3,
           TIP_NUM_TEL1,
           TIP_NUM_TEL2,
           TIP_NUM_TEL3,
           COD_TIPO_DEPENDENTE,
           FLG_ELABORAIS,
           COD_TIPO_END,
           PAIS_ID,
           NUM_CEP,
           COD_UF,
           COD_MUNICIPIO,
           COD_BAIRRO,
           COD_TIP_LOGRADOURO,
           NOM_LOGRADOURO,
           NUM_NUMERO,
           DES_COMPLEMENTO,
           DES_PONTO_REFERENCIA,
           NOM_CARTORIO_CASA,
           NOM_CARTORIO_NASC,
           FLG_END_DEPENDENTE,
           DES_JUSTIFICATIVA,
           COD_IDE_CLI_DEP)
           VALUES
           (V_DEP_ID,               --ID,
            R.DES_NATURAL,          --COD_CID_NASC,
            R.COD_EST_CIV,          --COD_EST_CIV,
            R.COD_PARENTESCO,       --COD_GRAU_PARENTESCO,
            I_COD_IDE_CLI,          --COD_IDE_CLI,
            I_COD_INS,              --COD_INS,
            R.COD_NACIO,            --COD_NACIO,
            R.COD_ORG_EMI_RG,       --COD_ORG_EMI_RG,
            v_cod_pais_nasc,        --COD_PAIS_NASC,
            R.COD_RACA,             --COD_RACA,
            R.COD_SEXO,             --COD_SEXO,
            v_cod_uf_emi_rg,        --COD_UF_EMI_RG,
            v_cod_uf_nasc,          --COD_UF_NASC,
            SYSDATE,                --DAT_CRIACAO,
            R.DAT_EMI_RG,           --DAT_EMI_RG,
            R.DAT_NASC,             --DAT_NASC,
            null,                   --DAT_RECADASTRAMENTO,
            null,                   --DAT_RECENSEAMENTO,
            SYSDATE,                --DAT_ULT_ATU,
            R.DES_EMAIL,            --EMAIL,
            null,                   --FLAG_RECADASTRAMENTO,
            null,                   --FLAG_RECENSEAMENTO,
            R.FLG_SAL_FAM,          --FLG_SAL_FAM,
            R.FLG_DEP_IR,           --HAS_DEPENDENTE_IR,
            R.NOM_PESSOA_FISICA,    --NOM_PESSOA_FISICA,
            'SP_CARGA_DEPENDENTES', --NOM_PRO_CRIACAO,
            'SP_CARGA_DEPENDENTES', --NOM_PRO_ULT_ATU,
            user,
            user,
            R.NUM_CARTORIO_CASA,    --NUM_CARTORIO_CASA,
            R.NUM_CARTORIO_NASC,    --NUM_CARTORIO_NASC,
            R.NUM_CPF,              --NUM_CPF,
            R.NUM_FOLHA_CASA,       --NUM_FOLHA_CASA,
            R.NUM_FOLHA_NASC,       --NUM_FOLHA_NASC,
            R.NUM_LIVRO_CASA,       --NUM_LIVRO_CASA,
            R.NUM_LIVRO_NASC,       --NUM_LIVRO_NASC,
            R.NUM_RG,               --NUM_RG,
            R.NUM_TEL_1,            --NUM_TEL1,
            R.NUM_TEL_2,            --NUM_TEL2,
            R.NUM_TEL_3,            --NUM_TEL3,
            R.TIP_NUM_TEL_1,        --TIP_NUM_TEL1,
            R.TIP_NUM_TEL_2,        --TIP_NUM_TEL2,
            R.TIP_NUM_TEL_3,        --TIP_NUM_TEL3,
            v_tipo_Dep,             --COD_TIPO_DEPENDENTE,
            R.FLG_DEF_LABORAIS,     --FLG_ELABORAIS,
            R.COD_TIPO_END,         --COD_TIPO_END,
            v_id_pais,              --PAIS_ID,
            R.NUM_CEP,              --NUM_CEP,
            v_cod_uf,               --COD_UF,
            v_cod_municipio,        --COD_MUNICIPIO,
            v_cod_bairro,           --COD_BAIRRO,
            R.COD_TIPO_LOGRADOURO,  --COD_TIP_LOGRADOURO,
            R.NOM_LOGRADOURO,       --NOM_LOGRADOURO,
            R.NUM_NUMERO,           --NUM_NUMERO,
            R.DES_COMPLEMENTO,      --DES_COMPLEMENTO,
            R.DES_PONTO_REFERENCIA, --DES_PONTO_REFERENCIA,
            null,                   --NOM_CARTORIO_CASA,
            null,                   --NOM_CARTORIO_NASC,
            null,                   --FLG_END_DEPENDENTE,
            null,                   --DES_JUSTIFICATIVA,
            R.COD_IDE_CLI_DEP       --COD_IDE_CLI_DEP             
           );    
        ELSE
          IF V_FLG_RECADASTRAMENTO = 0 and V_FLG_RECENSEAMENTO = 0 THEN 
            update tb_dependente
               set  COD_CID_NASC = R.DES_NATURAL,         
                    COD_EST_CIV  = R.COD_EST_CIV,         
                    COD_GRAU_PARENTESCO  = R.COD_PARENTESCO,                 
                    COD_NACIO  = R.COD_NACIO,           
                    COD_ORG_EMI_RG = R.COD_ORG_EMI_RG,      
                    COD_PAIS_NASC  = v_cod_pais_nasc,       
                    COD_RACA = R.COD_RACA,            
                    COD_SEXO = R.COD_SEXO,            
                    COD_UF_EMI_RG  = v_cod_uf_emi_rg,       
                    COD_UF_NASC  = v_cod_uf_nasc,         
                    DAT_CRIACAO  = SYSDATE,               
                    DAT_EMI_RG = R.DAT_EMI_RG,          
                    DAT_NASC = R.DAT_NASC,                                     
                    DAT_ULT_ATU  = SYSDATE,               
                    EMAIL  = R.DES_EMAIL,           
                    FLAG_RECADASTRAMENTO = null,                  
                    FLAG_RECENSEAMENTO = null,                  
                    FLG_SAL_FAM  = R.FLG_SAL_FAM,         
                    HAS_DEPENDENTE_IR  = R.FLG_DEP_IR,          
                    NOM_PESSOA_FISICA  = R.NOM_PESSOA_FISICA,   
                    NOM_PRO_CRIACAO  = 'SP_CARGA_DEPENDENTES',
                    NOM_PRO_ULT_ATU  = 'SP_CARGA_DEPENDENTES',
                    NOM_USU_CRIACAO  = user,
                    NOM_USU_ULT_ATU  = user,
                    NUM_CARTORIO_CASA  = R.NUM_CARTORIO_CASA,   
                    NUM_CARTORIO_NASC  = R.NUM_CARTORIO_NASC,             
                    NUM_FOLHA_CASA = R.NUM_FOLHA_CASA,      
                    NUM_FOLHA_NASC = R.NUM_FOLHA_NASC,      
                    NUM_LIVRO_CASA = R.NUM_LIVRO_CASA,      
                    NUM_LIVRO_NASC = R.NUM_LIVRO_NASC,      
                    NUM_RG = R.NUM_RG,              
                    NUM_TEL1 = R.NUM_TEL_1,           
                    NUM_TEL2 = R.NUM_TEL_2,           
                    NUM_TEL3 = R.NUM_TEL_3,           
                    TIP_NUM_TEL1 = R.TIP_NUM_TEL_1,       
                    TIP_NUM_TEL2 = R.TIP_NUM_TEL_2,       
                    TIP_NUM_TEL3 = R.TIP_NUM_TEL_3,       
                    COD_TIPO_DEPENDENTE  = v_tipo_Dep,            
                    FLG_ELABORAIS  = R.FLG_DEF_LABORAIS,    
                    COD_TIPO_END = R.COD_TIPO_END,        
                    PAIS_ID  = v_id_pais,             
                    NUM_CEP  = R.NUM_CEP,             
                    COD_UF = v_cod_uf,              
                    COD_MUNICIPIO  = v_cod_municipio,       
                    COD_BAIRRO = v_cod_bairro,          
                    COD_TIP_LOGRADOURO = R.COD_TIPO_LOGRADOURO, 
                    NOM_LOGRADOURO = R.NOM_LOGRADOURO,      
                    NUM_NUMERO = R.NUM_NUMERO,          
                    DES_COMPLEMENTO  = R.DES_COMPLEMENTO,     
                    DES_PONTO_REFERENCIA = R.DES_PONTO_REFERENCIA,
                    NOM_CARTORIO_CASA  = null,                  
                    NOM_CARTORIO_NASC  = null,                  
                    FLG_END_DEPENDENTE = null,                  
                    DES_JUSTIFICATIVA  = null     
              where COD_IDE_CLI = I_COD_IDE_CLI
                and COD_IDE_CLI_DEP = R.COD_IDE_CLI_DEP;
          END IF;
        END IF;                               
      END LOOP;
      --
      update tb_pessoa_fisica
         set qtd_dep = nvl(qtd_dep,v_qtd_dep_prev + v_qtd_dep_econ),
             qtd_dep_ir = nvl(qtd_dep_ir,v_qtd_dep_econ),
             qtd_dep_prev = nvl(qtd_dep_prev,v_qtd_dep_prev)
        where cod_ins = i_cod_ins
          and cod_ide_cli = i_cod_ide_cli; 
     
    END SP_CARGA_DEPENDENTE; 
    -----------------------------------------------------------------------------------------------
    PROCEDURE SP_CARGA_REPRESENTANTE(I_COD_INS     VARCHAR2,
                                     I_COD_IDE_CLI VARCHAR2)
    AS
      V_REP_ID NUMBER;
      V_FLG_RECADASTRAMENTO    NUMBER;
      V_FLG_RECENSEAMENTO      NUMBER;
      
      v_cod_est_civ     TB_REPRESENTANTE_LEGAL.COD_EST_CIV%type; 
      v_cod_org_emi_rg  TB_REPRESENTANTE_LEGAL.COD_ORG_EMI_RG%type; 
      v_cod_pais_nasc   TB_REPRESENTANTE_LEGAL.COD_PAIS_NASC%type;
      v_cod_uf_emi_rg   TB_REPRESENTANTE_LEGAL.COD_UF_EMI_RG%type;
      v_cod_uf_nasc     TB_REPRESENTANTE_LEGAL.COD_UF_NASC%type;
      v_id_pais         TB_REPRESENTANTE_LEGAL.PAIS_ID%type;    
      v_cod_uf          TB_REPRESENTANTE_LEGAL.COD_UF_ID%type;   
      v_cod_municipio   TB_REPRESENTANTE_LEGAL.COD_MUNICIPIO_ID%type;      
      v_cod_bairro      TB_REPRESENTANTE_LEGAL.COD_BAIRRO_ID%type;
    BEGIN       
      FOR R IN
      (SELECT COD_BAIRRO, 
              DES_NATURAL,           
              COD_EST_CIV,                                                                   
              Cod_Municipio,         
              COD_NACIO,              
              COD_ORG_EMI_RG, 
              COD_PAIS_NASC, 
              COD_SEXO,               
              COD_TIPO_LOGRADOURO,  
              COD_UF,    
              COD_UF_EMI_RG, 
              COD_UF_NASC, 
              DAT_EMI_RG,              
              DAT_NASC,                
              DES_COMPLEMENTO,       
              DES_EMAIL,             
              NOM_LOGRADOURO,        
              NOM_PESSOA_FISICA,     
              NUM_CEP,               
              NUM_CPF,               
              NUM_NUMERO,            
              NUM_RG,                
              NUM_TEL_1, 
              COD_PAIS,                          
              REP.COD_IDE_CLI,
              END.NOM_BAIRRO_CARREGADO
        FROM USER_IPESP.TB_PESSOA_RELACAO REP
            JOIN USER_IPESP.TB_PESSOA_FISICA PF 
              ON PF.COD_INS = REP.COD_INS
              AND PF.COD_IDE_CLI = REP.COD_IDE_CLI 
            LEFT JOIN USER_IPESP.TB_END_PESSOA_FISICA END
                   ON PF.COD_INS = END.COD_INS
                   AND PF.COD_IDE_CLI = END.COD_IDE_CLI
        WHERE REP.COD_IDE_CLI_RELACIONADO = I_COD_IDE_CLI
          AND COD_TIPO_RELACAO = USER_IPESP.PAC_INTEGR_UTIL.fnc_ret_tipo_repr_legal                    
      )      
      LOOP
        v_cod_pais_nasc  := user_ipesp.pac_integr_util.fnc_ret_depara_valor(R.COD_PAIS_NASC,5,2);
        v_cod_uf_emi_rg  := user_ipesp.pac_integr_util.fnc_ret_depara_valor(R.COD_UF_EMI_RG,10,2);
        v_cod_uf_nasc    := user_ipesp.pac_integr_util.fnc_ret_depara_valor(R.COD_UF_NASC,10,2);
        v_id_pais        := user_ipesp.pac_integr_util.fnc_ret_depara_valor(R.COD_PAIS,8,2);
        v_cod_uf         := user_ipesp.pac_integr_util.fnc_ret_depara_valor(R.COD_UF,10,2);
        v_cod_municipio  := user_ipesp.pac_integr_util.fnc_depara_bairro_mun_carga(R.COD_MUNICIPIO);
        v_cod_bairro     := user_ipesp.pac_integr_util.fnc_depara_bairro_mun_carga(R.COD_BAIRRO);                                                                                                                                                               
        --
        -- Verifica municipio e bairro pelo CEP
        v_char :=  FNC_IDENT_ID_BARRO_MUN(R.COD_UF, 
                                          R.NUM_CEP, 
                                          R.NOM_BAIRRO_CARREGADO, 
                                          v_cod_municipio,
                                          v_cod_bairro
                                          );
        --
        BEGIN 
        SELECT NVL(FLAG_RECADASTRAMENTO,0), NVL(FLAG_RECENSEAMENTO,0)
          INTO V_FLG_RECADASTRAMENTO, V_FLG_RECENSEAMENTO 
          FROM TB_REPRESENTANTE_LEGAL REP2
         WHERE REP2.COD_IDE_CLI = I_COD_IDE_CLI
           AND REP2.COD_IDE_CLI_REP  = R.COD_IDE_CLI;
        EXCEPTION 
          WHEN NO_DATA_FOUND THEN 
            V_FLG_RECADASTRAMENTO := null;
            V_FLG_RECENSEAMENTO    := null;    
        END;
        --
        IF V_FLG_RECADASTRAMENTO is null THEN 
          V_REP_ID := SEQ_EPRESENTANTE_LEGAL.NEXTVAL;
          INSERT INTO TB_REPRESENTANTE_LEGAL 
          ( ID,
            COD_BAIRRO_ID,
            COD_CID_NASC,
            COD_EST_CIV,
            COD_IDE_CLI,
            COD_INS,
            COD_MUNICIPIO_ID,
            COD_NACIO,
            COD_ORG_EMI_RG,
            COD_PAIS_NASC,
            COD_SEXO,
            COD_TEL_FONE1,
            COD_TIP_LOGRADOURO_ID,
            COD_UF_ID,
            COD_UF_EMI_RG,
            COD_UF_NASC,
            DAT_CRIACAO,
            DAT_EMI_RG,
            DAT_NASC,
            DAT_RECADASTRAMENTO,
            DAT_RECENSEAMENTO,
            DAT_ULT_ATU,
            DES_COMPLEMENTO,
            EMAIL,
            FLAG_RECADASTRAMENTO,
            FLAG_RECENSEAMENTO,
            NOM_LOGRADOURO,
            NOM_PESSOA_FISICA,
            NOM_PRO_CRIACAO,
            NOM_PRO_ULT_ATU,
            NOM_USU_CRIACAO,
            NOM_USU_ULT_ATU,
            NUM_CEP,
            NUM_CPF,
            NUM_NUMERO,
            NUM_RG,
            NUM_TEL_FONE1,
            PAIS_ID,
            COD_IDE_CLI_REP)
           VALUES
           (V_REP_ID,                --ID,
            v_cod_bairro,            --COD_BAIRRO_ID,
            R.DES_NATURAL,           --COD_CID_NASC,
            R.COD_EST_CIV,           -- OD_EST_CIV,
            I_COD_IDE_CLI,           --COD_IDE_CLI,
            I_COD_INS,               --COD_INS,
            v_cod_municipio,         --COD_MUNICIPIO_ID,
            R.COD_NACIO,             --COD_NACIO,
            R.COD_ORG_EMI_RG,        --COD_ORG_EMI_RG,
            v_cod_pais_nasc,         --COD_PAIS_NASC, 
            R.COD_SEXO,              --COD_SEXO,
            null,                    --COD_TEL_FONE1,
            R.COD_TIPO_LOGRADOURO,   --COD_TIP_LOGRADOURO_ID,
            v_cod_uf,                --COD_UF_ID,
            v_cod_uf_emi_rg,         --COD_UF_EMI_RG,
            v_cod_uf_nasc,           --COD_UF_NASC,
            SYSDATE,                 --DAT_CRIACAO,
            R.DAT_EMI_RG,            --DAT_EMI_RG,
            R.DAT_NASC,              --DAT_NASC,
            NULL,                    --DAT_RECADASTRAMENTO,
            NULL,                    --DAT_RECENSEAMENTO,
            NULL,                    --DAT_ULT_ATU,
            R.DES_COMPLEMENTO,       --DES_COMPLEMENTO,
            R.DES_EMAIL,             --EMAIL,
            NULL,                    --FLAG_RECADASTRAMENTO,
            NULL,                    --FLAG_RECENSEAMENTO,
            R.NOM_LOGRADOURO,        --NOM_LOGRADOURO,
            R.NOM_PESSOA_FISICA,     --NOM_PESSOA_FISICA,
            'SP_CARGA_REPRESENTANTE',--NOM_PRO_CRIACAO,
            'SP_CARGA_REPRESENTANTE',--NOM_PRO_ULT_ATU,
            user,                    --NOM_USU_CRIACAO,
            user,                    --NOM_USU_ULT_ATU,
            R.NUM_CEP,               --NUM_CEP,
            R.NUM_CPF,               --NUM_CPF,
            R.NUM_NUMERO,            --NUM_NUMERO,
            R.NUM_RG,                --NUM_RG,
            R.NUM_TEL_1,             --NUM_TEL_FONE1,
            v_id_pais,               --PAIS_ID,
            R.COD_IDE_CLI            --COD_IDE_CLI_REP        
           );  
         ELSE
           IF V_FLG_RECADASTRAMENTO = 0 and V_FLG_RECENSEAMENTO = 0 THEN 
           UPDATE TB_REPRESENTANTE_LEGAL
              SET COD_BAIRRO_ID = v_cod_bairro,            
                  COD_CID_NASC  = R.DES_NATURAL,           
                  COD_EST_CIV = R.COD_EST_CIV,                                      
                  COD_MUNICIPIO_ID  = v_cod_municipio,         
                  COD_NACIO = R.COD_NACIO,             
                  COD_ORG_EMI_RG  = R.COD_ORG_EMI_RG,        
                  COD_PAIS_NASC = v_cod_pais_nasc,         
                  COD_SEXO  = R.COD_SEXO,              
                  COD_TEL_FONE1 = null,                    
                  COD_TIP_LOGRADOURO_ID = R.COD_TIPO_LOGRADOURO,   
                  COD_UF_ID = v_cod_uf,                
                  COD_UF_EMI_RG = v_cod_uf_emi_rg,         
                  COD_UF_NASC = v_cod_uf_nasc,           
                  DAT_EMI_RG  = R.DAT_EMI_RG,            
                  DAT_NASC  = R.DAT_NASC,                                     
                  DAT_ULT_ATU = SYSDATE,                    
                  DES_COMPLEMENTO = R.DES_COMPLEMENTO,       
                  EMAIL = R.DES_EMAIL,                                  
                  NOM_LOGRADOURO  = R.NOM_LOGRADOURO,        
                  NOM_PESSOA_FISICA = R.NOM_PESSOA_FISICA,     
                  NOM_PRO_CRIACAO = 'SP_CARGA_REPRESENTANTE',
                  NOM_PRO_ULT_ATU = 'SP_CARGA_REPRESENTANTE',
                  NOM_USU_CRIACAO = user,                    
                  NOM_USU_ULT_ATU = user,                    
                  NUM_CEP = R.NUM_CEP,               
                  NUM_CPF = R.NUM_CPF,               
                  NUM_NUMERO  = R.NUM_NUMERO,            
                  NUM_RG  = R.NUM_RG,                
                  NUM_TEL_FONE1 = R.NUM_TEL_1,             
                  PAIS_ID = v_id_pais           
           WHERE COD_IDE_CLI = I_COD_IDE_CLI
             AND COD_IDE_CLI_REP = R.COD_IDE_CLI;    
           END IF;                                   
         END IF;            
      END LOOP;
    END SP_CARGA_REPRESENTANTE; 
    -----------------------------------------------------------------------------------------------
    PROCEDURE SP_CARGA_ENDERECO(I_COD_INS     VARCHAR2,
                                I_COD_IDE_CLI VARCHAR2)
    AS
      V_END_ID NUMBER;
      v_cod_uf  tb_end_pessoa_fisica.cod_uf%type;
      v_id_pais tb_end_pessoa_fisica.pais_id%type;
      v_cod_municipio varchar2(10); 
      v_cod_bairro    varchar2(10);
      V_FLG_RECADASTRAMENTO NUMBER;
      V_FLG_RECENSEAMENTO NUMBER;
      --
      v_char char;
    BEGIN    
      --      
         --         
      FOR E IN
      (SELECT COD_BAIRRO,                        
              COD_MUNICIPIO,       
              COD_TIPO_LOGRADOURO, 
              COD_UF,               
              DES_COMPLEMENTO,                   
              NOM_LOGRADOURO,                    
              NUM_CEP,             
              NUM_NUMERO,          
              COD_TIPO_END,        
              DES_PONTO_REFERENCIA,
              COD_PAIS,                   
              NUM_IDE_END_PF,
              NOM_BAIRRO_CARREGADO
         FROM USER_IPESP.TB_END_PESSOA_FISICA
        WHERE COD_INS = I_COD_INS
          AND COD_IDE_CLI = I_COD_IDE_CLI
      )      
      LOOP
        --v_cod_municipio  := user_ipesp.pac_integr_util.fnc_depara_bairro_mun_carga(E.COD_MUNICIPIO);
        --v_cod_bairro     := user_ipesp.pac_integr_util.fnc_depara_bairro_mun_carga(E.COD_BAIRRO);
        --
        v_cod_uf   := user_ipesp.pac_integr_util.fnc_ret_depara_valor(E.COD_UF,10,2);
        v_id_pais := user_ipesp.pac_integr_util.fnc_ret_depara_valor(E.COD_PAIS,8,2);                                                               
        --
        V_END_ID := SEQ_ND_PESSOA_FISICA.NEXTVAL;                  
        --
        -- Verifica municipio e bairro pelo CEP
        v_char :=  FNC_IDENT_ID_BARRO_MUN(E.COD_UF, 
                                          E.NUM_CEP, 
                                          E.NOM_BAIRRO_CARREGADO, 
                                          v_cod_municipio,
                                          v_cod_bairro
                                          );
                
        --            
        BEGIN 
          SELECT NVL(FLAG_RECADASTRAMENTO,0), NVL(FLAG_RECENSEAMENTO,0)
            INTO V_FLG_RECADASTRAMENTO, V_FLG_RECENSEAMENTO 
            FROM TB_END_PESSOA_FISICA
           WHERE COD_IDE_CLI = I_COD_IDE_CLI;
        EXCEPTION
          WHEN NO_DATA_FOUND THEN 
            V_FLG_RECADASTRAMENTO := NULL;  
            V_FLG_RECENSEAMENTO    := NULL;
        END;
         
        -- Não existe endereço para pessoa, pois V_FLG_RECADASTRAMENTO nunca pode ser nulo
        IF  V_FLG_RECADASTRAMENTO is null THEN  
          INSERT INTO TB_END_PESSOA_FISICA 
          (ID,
           COD_BAIRRO,
           COD_IDE_CLI,
           COD_INS,
           COD_MUNICIPIO,
           COD_TIP_LOGRADOURO,
           COD_UF,
           DAT_CRIACAO,
           DAT_RECADASTRAMENTO,
           DAT_RECENSEAMENTO,
           DAT_ULT_ATU,
           DES_COMPLEMENTO,
           EMAIL_INST,
           EMAIL_PART,
           FLAG_RECADASTRAMENTO,
           FLAG_RECENSEAMENTO,
           NOM_LOGRADOURO,
           NOM_PRO_CRIACAO,
           NOM_PRO_ULT_ATU,
           NOM_USU_CRIACAO,
           NOM_USU_ULT_ATU,
           NUM_CEP,
           NUM_NUMERO,        
           COD_TIPO_END,
           DES_PONTO_REFERENCIA,
           PAIS_ID              
           )
           VALUES
           (V_END_ID,              --ID,
            v_cod_bairro,          --COD_BAIRRO,
            I_COD_IDE_CLI,         --COD_IDE_CLI,
            I_COD_INS,             --COD_INS,
            v_cod_municipio,       --COD_MUNICIPIO,
            E.COD_TIPO_LOGRADOURO, --COD_TIP_LOGRADOURO,
            v_cod_uf,              --COD_UF,
            SYSDATE,               --DAT_CRIACAO,
            NULL,                  --DAT_RECADASTRAMENTO,
            NULL,                  --DAT_RECENSEAMENTO,
            SYSDATE,               --DAT_ULT_ATU,
            E.DES_COMPLEMENTO,     --DES_COMPLEMENTO,
            NULL,                  --EMAIL_INST,
            NULL,                  --EMAIL_PART,
            NULL,                  --FLAG_RECADASTRAMENTO,
            NULL,                  --FLAG_RECENSEAMENTO,
            E.NOM_LOGRADOURO,      --NOM_LOGRADOURO,
            'SP_CARGA_ENDERECO',   --NOM_PRO_CRIACAO,
            'SP_CARGA_ENDERECO',   --NOM_PRO_ULT_ATU,
            user,                  --NOM_USU_CRIACAO,
            user,                  --NOM_USU_ULT_ATU,
            E.NUM_CEP,             --NUM_CEP,
            E.NUM_NUMERO,          --NUM_NUMERO,
            603,                   --COD_TIPO_END, (Correspond)
            E.DES_PONTO_REFERENCIA,--DES_PONTO_REFERENCIA,
            v_id_pais             --PAIS_ID,     
            );      
          ELSE
            IF V_FLG_RECADASTRAMENTO = 0 and V_FLG_RECENSEAMENTO = 0 THEN 
              update TB_END_PESSOA_FISICA
                 set  COD_BAIRRO  = v_cod_bairro,        
                      COD_INS = I_COD_INS,             
                      COD_MUNICIPIO = v_cod_municipio,       
                      COD_TIP_LOGRADOURO  = E.COD_TIPO_LOGRADOURO, 
                      COD_UF  = v_cod_uf,              
                      DAT_CRIACAO = SYSDATE,               
                      DAT_RECADASTRAMENTO = NULL,                  
                      DAT_RECENSEAMENTO = NULL,                  
                      DAT_ULT_ATU = SYSDATE,               
                      DES_COMPLEMENTO = E.DES_COMPLEMENTO,     
                      EMAIL_INST  = NULL,                  
                      EMAIL_PART  = NULL,                  
                      FLAG_RECADASTRAMENTO  = NULL,                  
                      FLAG_RECENSEAMENTO  = NULL,                  
                      NOM_LOGRADOURO  = E.NOM_LOGRADOURO,      
                      NOM_PRO_CRIACAO = 'SP_CARGA_ENDERECO',   
                      NOM_PRO_ULT_ATU = 'SP_CARGA_ENDERECO',   
                      NOM_USU_CRIACAO = user,                  
                      NOM_USU_ULT_ATU = user,                  
                      NUM_CEP = E.NUM_CEP,             
                      NUM_NUMERO          = E.NUM_NUMERO,          
                      COD_TIPO_END  = 603,
                      DES_PONTO_REFERENCIA  = E.DES_PONTO_REFERENCIA,
                      PAIS_ID   = v_id_pais  
               WHERE  COD_IDE_CLI = I_COD_IDE_CLI; 
           END IF; 
         END IF;             
       END LOOP;
    EXCEPTION 
      WHEN OTHERS THEN 
        raise_application_error(-20000,'[SP_CARGA_ENDERECO] - '||sqlerrm); 
    END SP_CARGA_ENDERECO;  
    ----------------------------------------------------------------------------------
    PROCEDURE SP_CARGA_DADOS_RECADAST (i_cod_ins     in number,
                                       i_cod_ide_cli in varchar2,
                                       o_msg         in out varchar2)
    IS

        v_passo                 number := 0;
        v_qtd_cid_nasc          number;
        v_cod_cid_nasc          recenseamento.tb_pessoa_fisica.cod_cid_nasc%type;
        v_cod_pais_nasc         recenseamento.tb_pessoa_fisica.cod_pais_nasc%type;
        v_id_recen              recenseamento.tb_pessoa_fisica.id%type;
        v_usu_existe            varchar2(5);
        v_msg                   varchar2(4000);

    BEGIN

 
        v_passo := 0;

        if (v_role_user_id is null) then
            select id
            into v_role_user_id
            from recenseamento.tb_perfil
            where authority = 'ROLE_USER'; 
        end if;

        if ( fnc_perm_carga_pessoa_recad(i_cod_ins, i_cod_ide_cli) = 'TRUE' ) then

            v_passo := 1;

            for pf in (select a.num_cpf, 
                              a.cod_est_civ, 
                              a.cod_ide_cli, 
                              a.cod_ins, 
                              a.cod_nacio, 
                              a.cod_org_emi_rg, 
                              a.cod_raca, 
                              a.cod_reg_casamento, 
                              a.cod_sexo, 
                              a.cod_uf_emi_rg, 
                              a.cod_uf_nasc,
                              a.cod_pais_nasc, 
                              a.dat_emi_rg, 
                              a.dat_nasc, 
                              a.dat_obito, 
                              a.dat_recenseamento, 
                              a.des_email, 
                              null as flg_uniao_estavel,--a.flg_uniao_estavel, 
                              a.nom_mae, 
                              a.nom_pai, 
                              a.nom_pessoa_fisica, 
                              a.login,
                              a.num_cartorio_nasc, 
                              a.num_cer_res, 
                              a.num_folha_nasc, 
                              a.num_livro_nasc, 
                              a.num_rg, 
                              a.num_sec_ele, 
                              a.num_tel_1, 
                              a.num_tel_2, 
                              a.num_tit_ele,
                              a.num_zon_ele,
                              a.tip_num_tel_1, 
                              a.tip_num_tel_2, 
                              a.dat_cheg_pais, 
                              a.des_natural, 
                              a.dat_ult_atu,
                              b.num_nit_inss, 
                              b.num_pis,
                              c.num_dv_agencia, 
                              c.num_dv_conta, 
                              c.num_agencia, 
                              c.cod_banco, 
                              c.num_conta,
                              e.dat_nasc dat_nasc_conjuge, 
                              e.nom_pessoa_fisica nom_conjuge, 
                              e.num_cpf num_cpf_conjuge,
                              f.cod_ide_cli cod_ide_cli_recen, 
                              f.flag_recadastramento, 
                              f.dat_recadastramento, 
                              f.dat_ult_atu dat_ult_atu_recen, 
                              f.id id_recen
                       from user_ipesp.tb_pessoa_fisica a
                          left outer join user_ipesp.tb_servidor b
                              on      a.cod_ins     = b.cod_ins
                                  and a.cod_ide_cli = b.cod_ide_cli
                          left outer join user_ipesp.tb_informacao_bancaria c
                              on      a.cod_ins          = c.cod_ins
                                  and a.cod_ide_cli      = c.cod_ide_cli
                                  and c.cod_ide_inf_banc = (select min(cod_ide_inf_banc)
                                                            from user_ipesp.tb_informacao_bancaria c_min
                                                            where c_min.cod_ins     = a.cod_ins
                                                              and c_min.cod_ide_cli = a.cod_ide_cli)
                          left outer join user_ipesp.tb_dependente d
                              on      a.cod_ins     = d.cod_ins
                                  and a.cod_ide_cli = d.cod_ide_cli_serv
                                  and d.cod_parentesco = '00010'  -- TB_CODIGO: COD_NUM = 2015, COD_PAR = '00010', DES_DESCRICAO = 'Conjuge'
                          left outer join user_ipesp.tb_pessoa_fisica e 
                              on      d.cod_ins          = e.cod_ins
                                  and d.cod_ide_cli_dep = e.cod_ide_cli
                          left outer join recenseamento.tb_pessoa_fisica f
                              on      a.cod_ins     = f.cod_ins
                                  and a.cod_ide_cli = f.cod_ide_cli
                       where a.cod_ins     = i_cod_ins
                         and a.cod_ide_cli = i_cod_ide_cli
                         /* Exige que o registro de tb_pessoa_fisica esteja regular em tb_beneficiario */
                         and exists (select 'x'
                                     from user_ipesp.tb_beneficiario g
                                     where g.cod_ins = i_cod_ins
                                       and g.cod_ide_cli_ben = i_cod_ide_cli
                                       and upper(g.flg_status) in ('A', 'S')
                                       and nvl(g.dat_fim_ben, (sysdate + 1)) > sysdate)
                       ) loop

                v_qtd_cid_nasc := 0;

                select count(*) 
                into v_qtd_cid_nasc
                from user_ipesp.tb_municipio
                where upper(nom_municipio) = upper(pf.des_natural);

                if (v_qtd_cid_nasc = 1) then
                    select cod_municipio
                    into v_cod_cid_nasc
                    from user_ipesp.tb_municipio
                    where upper(nom_municipio) = upper(pf.des_natural);
                else
                    v_cod_cid_nasc := null;
                end if;
                --
                v_cod_pais_nasc :=  user_ipesp.pac_integr_util.fnc_ret_depara_valor(PF.COD_PAIS_NASC,10,2);
                
               
                if (pf.cod_ide_cli_recen is null) then -- registro nao existe em recenseamento.tb_pessoa_fisica

                    v_passo := 2;
                    v_id_recen := recenseamento.seq_essoa_fisica.nextval;

                    insert into recenseamento.tb_pessoa_fisica
                            (id,                     -- 1
                             cat_cnh,                -- 2
                             cod_cid_nasc,           -- 3
                             cod_cla_condtrab,       -- 4
                             cod_est_civ,            -- 5
                             cod_ide_cli,            -- 6
                             cod_ins,                -- 7
                             cod_nacio,              -- 8
                             cod_org_emi_rg,         -- 9
                             cod_pais_nasc,          -- 10
                             cod_raca,               -- 11
                             cod_sexo,               -- 12
                             cod_uf_cnh,             -- 13
                             cod_uf_emi_rg,          -- 14
                             cod_uf_nasc,            -- 15
                             dat_cheg_est,           -- 16
                             dat_criacao,            -- 17
                             dat_emi_rg,             -- 18
                             dat_exp_cnh,            -- 19
                             dat_exp_est,            -- 20
                             dat_first_cnh,          -- 21
                             dat_nasc,               -- 22
                             dat_nasc_conjuge,       -- 23
                             dat_obito,              -- 24
                             dat_recadastramento,    -- 25
                             dat_recenseamento,      -- 26
                             dat_ult_atu,            -- 27
                             dat_val_cnh,            -- 28
                             dv_agencia,             -- 29
                             dv_conta,               -- 30
                             email,                  -- 31
                             flag_recadastramento,   -- 32
                             flag_recenseamento,     -- 33
                             flag_uniao_estavel,     -- 34
                             flg_cas_bra,            -- 35
                             flg_def_auditiva,       -- 36
                             flg_def_fisica,         -- 37
                             flg_def_intelectual,    -- 38
                             flg_def_mental,         -- 39
                             flg_def_visual,         -- 40
                             flg_fil_bra,            -- 41
                             foto,                   -- 42
                             mom_mae,                -- 43
                             mom_pai,                -- 44
                             nom_cargo,              -- 45
                             nom_conjuge,            -- 46
                             nom_pessoa_fisica,      -- 47
                             nom_pro_criacao,        -- 48
                             nom_pro_ult_atu,        -- 49
                             nom_social,             -- 50
                             nom_usu_criacao,        -- 51
                             nom_usu_ult_atu,        -- 52
                             num_agencia,            -- 53
                             num_banco,              -- 54
                             num_cartorio_casa,      -- 55
                             num_cartorio_nasc,      -- 56
                             num_cer_res,            -- 57
                             num_cnh,                -- 58
                             num_conta,              -- 59
                             num_cpf,                -- 60
                             num_cpf_conjuge,        -- 61
                             num_folha_casa,         -- 62
                             num_folha_nasc,         -- 63
                             num_ins_reg_nac_est,    -- 64
                             num_livro_casa,         -- 65
                             num_livro_nasc,         -- 66
                             num_org_emi_est,        -- 67
                             num_rg,                 -- 68
                             num_sec_ele,            -- 69
                             num_til_ele,            -- 70
                             num_zon_ele,            -- 71
                             cod_reg_casamento,      -- 72
                             num_pis,                -- 73
                             num_nit,                -- 74
                             flg_elaborais,          -- 75
                             nom_cartorio,           -- 76
                             num_tel_fone1,          -- 77
                             num_tel_fone2,          -- 78
                             cod_tel_fone1,          -- 79
                             cod_tel_fone2)          -- 80
                        values 
                            (v_id_recen,                    -- 1 id                     
                             null,                          -- 2 cat_cnh             
                             v_cod_cid_nasc,                -- 3 cod_cid_nasc
                             null,                          -- 4 cod_cla_condtrab
                             pf.cod_est_civ,                -- 5 cod_est_civ        
                             pf.cod_ide_cli,                -- 6 cod_ide_cli     
                             pf.cod_ins,                    -- 7 cod_ins
                             pf.cod_nacio,                  -- 8 cod_nacio
                             pf.cod_org_emi_rg,             -- 9 cod_org_emi_rg
                             v_cod_pais_nasc,               -- 10 cod_pais_nasc
                             pf.cod_raca,                   -- 11 cod_raca
                             pf.cod_sexo,                   -- 12 cod_sexo
                             null,                          -- 13 cod_uf_cnh
                             pf.cod_uf_emi_rg,              -- 14 cod_uf_emi_rg
                             substr(pf.cod_uf_nasc, 1, 2),  -- 15 cod_uf_nasc
                             pf.dat_cheg_pais,              -- 16 dat_cheg_est
                             sysdate,                       -- 17 dat_criacao
                             pf.dat_emi_rg,                 -- 18 dat_emi_rg
                             null,                          -- 19 dat_exp_cnh
                             null,                          -- 20 dat_exp_est
                             null,                          -- 21 dat_first_cnh
                             pf.dat_nasc,                   -- 22 dat_nasc
                             pf.dat_nasc_conjuge,           -- 23 dat_nasc_conjuge
                             pf.dat_obito,                  -- 24 dat_obito
                             null,                          -- 25 dat_recadastramento
                             null,                          -- 26 dat_recenseamento
                             sysdate,                       -- 27 dat_ult_atu
                             null,                          -- 28 dat_val_cnh
                             pf.num_dv_agencia,             -- 29 dv_agencia
                             pf.num_dv_conta,               -- 30 dv_conta
                             pf.des_email,                  -- 31 email
                             null,                          -- 32 flag_recadastramento
                             null,                          -- 33 flag_recenseamento
                             null, --nvl(pf.flg_uniao_estavel, 'N'), -- 34 flag_uniao_estavel
                             null,                          -- 35 flg_cas_bra
                             null,                          -- 36 flg_def_auditiva
                             null,                          -- 37 flg_def_fisica
                             null,                          -- 38 flg_def_intelectual
                             null,                          -- 39 flg_def_mental
                             null,                          -- 40 flg_def_visual
                             null,                          -- 41 flg_fil_bra
                             null,                          -- 42 foto
                             pf.nom_mae,                    -- 43 mom_mae
                             pf.nom_pai,                    -- 44 mom_pai
                             null,                          -- 45 nom_cargo
                             pf.nom_conjuge,                -- 46 nom_conjuge
                             pf.nom_pessoa_fisica,          -- 47 nom_pessoa_fisica
                             'PAC_RECADAST_NOVAPREV.SP_CARGA_DADOS_RECADAST',     -- 48 nom_pro_criacao
                             'PAC_RECADAST_NOVAPREV.SP_CARGA_DADOS_RECADAST',     -- 49 nom_pro_ult_atu
                             null,                          -- 50 nom_social
                             user,                          -- 51 nom_usu_criacao
                             user,                          -- 52 nom_usu_ult_atu
                             pf.num_agencia,                -- 53 num_agencia
                             pf.cod_banco,                  -- 54 num_banco
                             null,                          -- 55 num_cartorio_casa
                             pf.num_cartorio_nasc,          -- 56 num_cartorio_nasc
                             pf.num_cer_res,                -- 57 num_cer_res
                             null,                          -- 58 num_cnh
                             pf.num_conta,                  -- 59 num_conta
                             pf.num_cpf,                    -- 60 num_cpf
                             pf.num_cpf_conjuge,            -- 61 num_cpf_conjuge
                             null,                          -- 62 num_folha_casa
                             pf.num_folha_nasc,             -- 63 num_folha_nasc
                             null,                          -- 64 num_ins_reg_nac_est
                             null,                          -- 65 num_livro_casa
                             pf.num_livro_nasc,             -- 66 num_livro_nasc
                             null,                          -- 67 num_org_emi_est
                             pf.num_rg,                     -- 68 num_rg
                             pf.num_sec_ele,                -- 69 num_sec_ele
                             pf.num_tit_ele,                -- 70 num_til_ele
                             pf.num_zon_ele,                -- 71 num_zon_ele
                             pf.cod_reg_casamento,          -- 72 cod_reg_casamento
                             pf.num_pis,                    -- 73 num_pis
                             pf.num_nit_inss,               -- 74 num_nit
                             null,                          -- 75 flg_elaborais
                             null,                          -- 76 nom_cartorio
                             pf.num_tel_1,                  -- 77 num_tel_fone1
                             pf.num_tel_2,                  -- 78 num_tel_fone2
                             pf.tip_num_tel_1,              -- 79 cod_tel_fone1
                             pf.tip_num_tel_2);             -- 80 cod_tel_fone2

                else -- registro existe em recenseamento.tb_pessoa_fisica

                    v_id_recen := pf.id_recen;

                    if ((nvl(pf.flag_recadastramento, 0) = 0)
                         and (pf.dat_recadastramento is null) 
                         --and (nvl(pf.dat_ult_atu_recen, sysdate) < nvl(pf.dat_ult_atu, (sysdate + 1)))
                         ) then

                        v_passo := 3;
                        -- nao foi efetuado o recadastramento e o registro de recenseamento e mais antigo que o de user_ipesp
                        update recenseamento.tb_pessoa_fisica
                        set cod_cid_nasc       = v_cod_cid_nasc,                -- 3            
                            cod_est_civ        = pf.cod_est_civ,                -- 5         
                            cod_nacio          = pf.cod_nacio,                  -- 8 
                            cod_org_emi_rg     =  pf.cod_org_emi_rg,            -- 9 
                            cod_pais_nasc      = v_cod_pais_nasc,               -- 10 
                            cod_raca           = pf.cod_raca,                   -- 11               
                            cod_sexo           = pf.cod_sexo,                   -- 12               
                            cod_uf_emi_rg      = pf.cod_uf_emi_rg,              -- 14          
                            cod_uf_nasc        = pf.cod_uf_nasc,                -- 15            
                            dat_cheg_est       = pf.dat_cheg_pais,              -- 16           
                            dat_emi_rg         = pf.dat_emi_rg,                 -- 18             
                            dat_nasc           = pf.dat_nasc,                   -- 22               
                            dat_nasc_conjuge   = pf.dat_nasc_conjuge,           -- 23       
                            dat_obito          = pf.dat_obito,                  -- 24              
                            dat_ult_atu        = sysdate,                       -- 27            
                            dv_agencia         = pf.num_dv_agencia,             -- 29             
                            dv_conta           = pf.num_dv_conta,               -- 30               
                            email              = pf.des_email,                  -- 31                  
                            flag_uniao_estavel = null, --nvl(pf.flg_uniao_estavel, 'N'), -- 34     
                            mom_mae            = pf.nom_mae,                    -- 43                
                            mom_pai            = pf.nom_pai,                    -- 44                
                            nom_conjuge        = pf.nom_conjuge,                -- 46            
                            nom_pessoa_fisica  = pf.nom_pessoa_fisica,          -- 47      
                            nom_pro_ult_atu    = 'PAC_RECADAST_NOVAPREV.SP_CARGA_DADOS_RECADAST', -- 49        
                            nom_usu_ult_atu    = user,                          -- 52        
                            num_agencia        = pf.num_agencia,                -- 53            
                            num_banco          = pf.cod_banco,                  -- 54              
                            num_cartorio_nasc  = pf.num_cartorio_nasc,          -- 56      
                            num_cer_res        = pf.num_cer_res,                -- 57            
                            num_conta          = pf.num_conta,                  -- 59              
                            num_cpf            = pf.num_cpf,                    -- 60                
                            num_cpf_conjuge    = pf.num_cpf_conjuge,            -- 61        
                            num_folha_nasc     = pf.num_folha_nasc,             -- 63         
                            num_livro_nasc     = pf.num_livro_nasc,             -- 66         
                            num_rg             = pf.num_rg,                     -- 68                 
                            num_sec_ele        = pf.num_sec_ele,                -- 69            
                            num_til_ele        = pf.num_tit_ele,                -- 70            
                            num_zon_ele        = pf.num_zon_ele,                -- 71            
                            cod_reg_casamento  = pf.cod_reg_casamento,          -- 72      
                            num_pis            = pf.num_pis,                    -- 73                
                            num_nit            = pf.num_nit_inss,               -- 74                
                            num_tel_fone1      = pf.num_tel_1,                  -- 77          
                            num_tel_fone2      = pf.num_tel_2,                  -- 78 
                            cod_tel_fone1      = pf.tip_num_tel_1,              -- 79 
                            cod_tel_fone2      = pf.tip_num_tel_2               -- 80 
                        where id = pf.id_recen;

                    else

                        recenseamento.sp_registra_log_bd('PAC_RECADAST_NOVAPREV.SP_CARGA_DADOS_RECADAST',
                                                        'COD_INS:'||i_cod_ins||', COD_IDE_CLI:'||i_cod_ide_cli||', passo '||v_passo,
                                                        'ja foi efetuado o recadastramento ou registro de'||
                                                            ' recenseamento.tb_pessoa_fisica nao e mais antigo'||
                                                            ' que o de user_ipesp.tb_pessoa_fisica'||
                                                            '(flag_recadastramento: '||nvl(to_char(pf.flag_recadastramento), '-nulo-')||
                                                            ', dat_recadastramento: '||nvl(to_char(pf.dat_recadastramento, 'dd/mm/yyyy'), '-nulo-')||
                                                            ', dat_ult_atu - recenseamento: '||to_char(nvl(pf.dat_ult_atu_recen, sysdate), 'dd/mm/yyyy')||
                                                            ', dat_ult_atu - user_ipesp: '||to_char(nvl(pf.dat_ult_atu, sysdate), 'dd/mm/yyyy')||')');

                    end if;

                end if;
                v_passo := 4;
                -- Adiciona dependente, endereço e representantes
                SP_CARGA_DEPENDENTE(pf.cod_ins,pf.cod_ide_cli);
                SP_CARGA_ENDERECO(pf.cod_ins,pf.cod_ide_cli);
                SP_CARGA_REPRESENTANTE(pf.cod_ins,pf.cod_ide_cli);                                      

                v_passo := 5;

                begin
                    v_usu_existe := 'FALSE';

                    select 'TRUE'
                    into v_usu_existe
                    from recenseamento.tb_usuario
                    where pessoa_fisica_id = v_id_recen
                      and rownum = 1;
                exception
                    when no_data_found then
                        null;
                    when others then
                        recenseamento.sp_registra_log_bd('PAC_RECADAST_NOVAPREV.SP_CARGA_DADOS_RECADAST',
                                                        'COD_INS:'||i_cod_ins||', COD_IDE_CLI:'||i_cod_ide_cli||', passo '||v_passo,
                                                        'Erro ao pesquisar usuario: '||sqlerrm);
                    raise;
                end;

                if (v_usu_existe = 'FALSE') then -- usuario nao esta cadastrado

                    SP_REGISTRA_ACESSO_USUARIO (v_id_recen, nvl(pf.login, pf.num_cpf), v_msg);

                    if (v_msg is not null) then

                        recenseamento.sp_registra_log_bd('PAC_RECADAST_NOVAPREV.SP_CARGA_DADOS_RECADAST',
                                                            'COD_INS:'||i_cod_ins||', COD_IDE_CLI:'||i_cod_ide_cli||', passo '||v_passo,
                                                            'SP_REGISTRA_ACESSO_USUARIO('||v_id_recen||', '||nvl(pf.login, pf.num_cpf)||'): '||v_msg);

                    end if;

                else    -- usuario ja cadastrado, nao altera nada

                    null;

                end if;

            end loop;
            
            commit;

            v_passo := 7;
            for bf in (select a.cod_beneficio, 
                             a.cod_ide_cli_ben, 
                             a.cod_ins, 
                             a.dat_fim_ben, 
                             a.dat_ini_ben, 
                             a.flg_status,
                             a.cod_processo as num_prontuario, 
                             a.val_percentual, 
                             a.dat_ult_atu,
                             b.cod_entidade, 
                             b.cod_tipo_beneficio, 
                             b.val_percent_ben, 
                             b.dat_concessao,
                             0 as val_percent_rateio, --c.val_percent_rateio,
                             d.cod_ide_cli cod_ide_cli_ben_recen, 
                             d.flag_recadastramento, 
                             d.dat_recadastramento, 
                             d.dat_ult_atu dat_ult_atu_recen, 
                             d.cod_beneficio cod_beneficio_recen, 
                             d.id id_recen,
                             null as flg_complemento_inss -- Regra
                         from user_ipesp.tb_beneficiario a inner join user_ipesp.tb_concessao_beneficio b
                                                                   on  b.cod_ins       = a.cod_ins
                                                                   and b.cod_beneficio = a.cod_beneficio                        
                                                       left outer join recenseamento.tb_beneficiario d
                                                                    on  a.cod_ins         = d.cod_ins
                                                                    and a.cod_ide_cli_ben = d.cod_ide_cli
                                                                    and a.cod_beneficio   = d.cod_beneficio

                         where a.cod_ins = i_cod_ins
                           and a.cod_ide_cli_ben = i_cod_ide_cli
                           and upper(a.flg_status) in ('A', 'S')
                           and nvl(a.dat_fim_ben, (sysdate + 1)) > sysdate
                       ) loop

                -- 
                if (bf.cod_ide_cli_ben_recen is null) then -- registro nao existe em recenseamento.tb_beneficiario

                    v_passo := 8;
                    insert into recenseamento.tb_beneficiario
                            (id,                    -- 1
                             cod_ide_cli,           -- 2
                             cod_tipo_beneficio,    -- 3
                             dat_fim_beneficio,     -- 4
                             dat_ini_beneficio,     -- 5
                             flg_status,            -- 6
                             num_prontuario,        -- 7
                             porc_beneficio,        -- 8
                             porc_rateio,           -- 9
                             cod_ins,               -- 10
                             cod_entidade,          -- 11
                             dat_criacao,           -- 12
                             nom_pro_criacao,       -- 13
                             nom_pro_ult_atu,       -- 14
                             nom_usu_criacao,       -- 15
                             nom_usu_ult_atu,       -- 16
                             dat_recadastramento,   -- 17
                             dat_recenseamento,     -- 18
                             dat_ult_atu,           -- 19
                             flag_recadastramento,  -- 20
                             flag_recenseamento,    -- 21
                             cod_beneficio,         -- 22
                             flg_complemento_inss,  -- 23
                             flg_peculio,           -- 24
                             dat_concessao)         -- 25
                        values
                            (recenseamento.seq_eneficiario.nextval, -- 1  id
                             bf.cod_ide_cli_ben,                -- 2  cod_ide_cli
                             DECODE(bf.cod_tipo_beneficio, 'V', '4', 
                                                           'G', '4',
                                                           'I', '3', 
                                                           'M', '1', 
                                                           'T', '6',  
                                                           'C', '2', 
                                                           'P', '5', 
                                                           bf.cod_tipo_beneficio), -- 3  cod_tipo_beneficio
                             bf.dat_fim_ben,                    -- 4  dat_fim_beneficio
                             bf.dat_ini_ben,                    -- 5  dat_ini_beneficio
                             DECODE(bf.flg_status, 'V', '1', 
                                                   'A', '1', 
                                                   'X', '2', 
                                                   'E', '2',  
                                                   'S', '3', 
                                                   bf.flg_status), -- 6  flg_status
                             bf.num_prontuario,                 -- 7  num_prontuario
                             bf.val_percentual,                 -- 8  porc_beneficio
                             nvl(bf.val_percent_rateio, 100),   -- 9  porc_rateio
                             bf.cod_ins,                        -- 10 cod_ins
                             bf.cod_entidade,                   -- 11 cod_entidade
                             sysdate,                           -- 12 dat_criacao
                             'PAC_RECADAST_NOVAPREV.SP_CARGA_DADOS_RECADAST',         -- 13 nom_pro_criacao
                             'PAC_RECADAST_NOVAPREV.SP_CARGA_DADOS_RECADAST',         -- 14 nom_pro_ult_atu
                             user,                              -- 15 nom_usu_criacao
                             user,                              -- 16 nom_usu_ult_atu
                             sysdate,                           -- 17 dat_recadastramento
                             sysdate,                           -- 18 dat_recenseamento
                             sysdate,                           -- 19 dat_ult_atu
                             1,                                 -- 20 flag_recadastramento
                             1,                                 -- 21 flag_recenseamento
                             bf.cod_beneficio,                  -- 22 cod_beneficio
                             bf.flg_complemento_inss,           -- 23 flg_complemento_inss
                             null,                            -- 24 flg_peculio
                             bf.dat_concessao);                 -- 25 dat_concessao

                else -- registro existe em recenseamento.tb_beneficiario

                    if (     (nvl(bf.flag_recadastramento, 0) = 0)
                         and (bf.dat_recadastramento is null) 
                       --  and (nvl(bf.dat_ult_atu_recen, sysdate) < nvl(bf.dat_ult_atu, (sysdate + 1)) ) 
                       ) then

                        v_passo := 9;
                        -- nao foi efetuado o recadastramento e o registro de recenseamento e mais antigo que o de user_ipesp
                        update recenseamento.tb_beneficiario
                        --set cod_tipo_beneficio = bf.cod_tipo_beneficio,           -- 3  
                        set cod_tipo_beneficio   = DECODE(bf.cod_tipo_beneficio, 'V', '4', 
                                                                                 'I', '3', 
                                                                                 'M', '1', 
                                                                                 'T', '6',  
                                                                                 'C', '2', 
                                                                                 'P', '5', 
                                                                                 bf.cod_tipo_beneficio), -- 3 
                            dat_fim_beneficio    = bf.dat_fim_ben,                  -- 4  
                            dat_ini_beneficio    = bf.dat_ini_ben,                  -- 5  
                            --flg_status = bf.flg_status,                           -- 6  
                            flg_status           = DECODE(bf.flg_status, 'V', '1', 
                                                                         'A', '1', 
                                                                         'X', '2', 
                                                                         'E', '2',  
                                                                         'S', '3', 
                                                                         bf.flg_status), -- 6
                            num_prontuario       = null,--bf.num_prontuario,               -- 7  
                            porc_beneficio       = bf.val_percentual,               -- 8  
                            porc_rateio          = nvl(bf.val_percent_rateio, 100), -- 9  
                            cod_entidade         = bf.cod_entidade,                 -- 11 
                            nom_pro_ult_atu      = 'PAC_RECADAST_NOVAPREV.SP_CARGA_DADOS_RECADAST',    -- 14 
                            nom_usu_ult_atu      = user,                            -- 16 
                            dat_ult_atu          = sysdate,                         -- 19 
                            flg_complemento_inss = bf.flg_complemento_inss,         -- 23 
                            flg_peculio          = null, -- bf.flg_peculio,                  -- 24
                            dat_concessao        = bf.dat_concessao                 -- 25
                        where id = bf.id_recen;

                    else

                        recenseamento.sp_registra_log_bd('PAC_RECADAST_NOVAPREV.SP_CARGA_DADOS_RECADAST',
                                                        'COD_INS:'||i_cod_ins||', COD_IDE_CLI:'||i_cod_ide_cli||', passo '||v_passo,
                                                        'ja foi efetuado o recadastramento ou registro de'||
                                                            ' recenseamento.tb_beneficiario nao e mais antigo'||
                                                            ' que o de user_ipesp.tb_beneficiario'||
                                                            '(flag_recadastramento: '||nvl(to_char(bf.flag_recadastramento), '-nulo-')||
                                                            ', dat_recadastramento: '||nvl(to_char(bf.dat_recadastramento, 'dd/mm/yyyy'), '-nulo-')||
                                                            ', dat_ult_atu - recenseamento: '||to_char(nvl(bf.dat_ult_atu_recen, sysdate), 'dd/mm/yyyy')||
                                                            ', dat_ult_atu - user_ipesp: '||to_char(nvl(bf.dat_ult_atu, sysdate), 'dd/mm/yyyy')||')');

                    end if;

                end if;

            end loop;

        end if;

    EXCEPTION
        WHEN OTHERS THEN    

            rollback;

            o_msg := substr('Erro processando COD_INS:'||i_cod_ins||', COD_IDE_CLI:'||i_cod_ide_cli||', passo '||v_passo||': '||
                           sqlerrm, 1, 1024);
            recenseamento.sp_registra_log_bd('PAC_RECADAST_NOVAPREV.SP_CARGA_DADOS_RECADAST',
                                             'COD_INS:'||i_cod_ins||', COD_IDE_CLI:'||i_cod_ide_cli||', passo '||v_passo,
                                              sqlerrm);

    
            commit;
    END    SP_CARGA_DADOS_RECADAST;

    -----------------------------------------------------------------------------------------------

    PROCEDURE SP_REGISTRA_ACESSO_USUARIO (i_pessoa_fisica_id in recenseamento.tb_usuario.pessoa_fisica_id%type,
                                          i_username         in recenseamento.tb_usuario.username%type,
                                          o_msg              in out varchar2) IS

        v_id_usuario    recenseamento.tb_usuario.id%type;
        v_pswd          recenseamento.tb_usuario."password"%type := '$2a$10$6FbHf.UtrH5p./Mnmh.VLeSOl7E2HntXyqKpi3dWzZSPsFUpIQrVW';

    BEGIN

        if (i_pessoa_fisica_id is null) then

            o_msg := 'Identificac?o da pessoa fisica n?o foi fornecida. Impossivel fazer o cadastro do usuario.';

        elsif (i_username is null) then

            o_msg := 'Login nao esta preenchido. Impossivel fazer o cadastro do usuario';

        elsif (v_role_user_id is null) then

            o_msg := 'Identificac?o da permiss?o do usuario nao esta preenchida. Impossivel fazer o cadastro do usuario';

        else

            v_id_usuario := recenseamento.seq_suario.nextval;

            insert into recenseamento.tb_usuario
                (id,                    -- 1
                 account_expired,       -- 2
                 account_locked,        -- 3
                 active,                -- 4
                 dat_term_resp,         -- 5
                 enabled,               -- 6
                 flag_term_resp,        -- 7
                 "password",              -- 8
                 password_expired,      -- 9
                 pessoa_fisica_id,      -- 10
                 username,              -- 11
                 dat_final_pre_cad,     -- 12
                 flag_final_pre_cad)    -- 13
            values
                (v_id_usuario,          -- 1 id
                 0,                     -- 2 account_expired
                 0,                     -- 3 account_locked
                 1,                     -- 4 active
                 null,                  -- 5 dat_term_resp
                 1,                     -- 6 enabled
                 null,                  -- 7 flag_term_resp
                 v_pswd,                -- 8 password
                 0,                     -- 9 password_expired
                 i_pessoa_fisica_id,    -- 10 pessoa_fisica_id
                 i_username,            -- 11 username
                 null,                  -- 12 dat_final_pre_cad
                 null);                 -- 13 flag_final_pre_cad

            insert into recenseamento.tb_usuario_perfil
                (perfil_id,      usuario_id)
            values
                (v_role_user_id, v_id_usuario);

        end if;

    EXCEPTION
        WHEN OTHERS THEN    

            o_msg := sqlerrm;

    END    SP_REGISTRA_ACESSO_USUARIO;


    -----------------------------------------------------------------------------------------------

    FUNCTION FNC_PERM_CARGA_PESSOA_RECAD (i_cod_ins     in number,
                                          i_cod_ide_cli in varchar2)
        RETURN VARCHAR2                                                                        
        IS

        function veri_dat_nasc_nula(i_cod_ins_vdnn in varchar2,
                                    i_cod_ide_cli_vdnn in varchar2) return varchar2 is

            v_dat_nasc user_ipesp.tb_pessoa_fisica.dat_nasc%type;
            v_retorno  varchar2(5) := 'TRUE';

        begin

            begin
                select dat_nasc
                into v_dat_nasc
                from user_ipesp.tb_pessoa_fisica
                where cod_ins     = i_cod_ins_vdnn
                  and cod_ide_cli = i_cod_ide_cli_vdnn;
            exception
                when others then
                    null;
            end;

            if ( v_dat_nasc is null) then

                recenseamento.sp_registra_log_bd('PAC_RECADAST_NOVAPREV.FNC_PERM_CARGA_PESSOA_RECAD',
                                                  'COD_INS:'||i_cod_ins_vdnn||', COD_IDE_CLI:'||i_cod_ide_cli_vdnn,
                                                  'Data de nascimento nao esta preenchida');
                v_retorno := 'FALSE';

            end if;

            return (v_retorno);

        end;

        function veri_recad_pessoa_fisica(i_cod_ins_vrcp     in number,
                                          i_cod_ide_cli_vrcp in varchar2) return varchar2 is

            v_flag_recadastramento recenseamento.tb_pessoa_fisica.flag_recadastramento%type;
            v_dat_recadastramento  recenseamento.tb_pessoa_fisica.dat_recadastramento%type;
            v_retorno              varchar2(5) := 'TRUE';

        begin

            begin
                select flag_recadastramento, dat_recadastramento
                into v_flag_recadastramento, v_dat_recadastramento
                from recenseamento.tb_pessoa_fisica
                where cod_ins     = i_cod_ins_vrcp
                  and cod_ide_cli = i_cod_ide_cli_vrcp;
            exception
                when others then
                    null;
            end;

            if (   (nvl(v_flag_recadastramento, 0) > 0) or (v_dat_recadastramento is not null) ) then

                recenseamento.sp_registra_log_bd('PAC_RECADAST_NOVAPREV.FNC_PERM_CARGA_PESSOA_RECAD',
                                                  'COD_INS:'||i_cod_ins_vrcp||', COD_IDE_CLI:'||i_cod_ide_cli_vrcp,
                                                  'Recadastramento iniciado/realizado para pessoa fisica: flag_recadastramento='||
                                                        nvl(to_char(v_flag_recadastramento), '-nulo-') ||
                                                        ', dat_recadastramento='|| 
                                                        nvl(to_char(v_dat_recadastramento, 'dd/mm/yyyy hh24:mi'), '-nulo-') );
                v_retorno := 'FALSE';

            end if;

            return (v_retorno);

        end;

        function veri_recad_beneficiario(i_cod_ins_vrb in number,
                                         i_cod_ide_cli_vrb in varchar2) return varchar2 is

            v_flag_recadastramento recenseamento.tb_beneficiario.flag_recadastramento%type;
            v_dat_recadastramento  recenseamento.tb_beneficiario.dat_recadastramento%type;
            v_retorno              varchar2(5) := 'TRUE';

        begin

            for bn in (select flag_recadastramento, dat_recadastramento
                       from recenseamento.tb_beneficiario
                       where cod_ins     = i_cod_ins_vrb
                         and cod_ide_cli = i_cod_ide_cli_vrb
                       ) loop 

                if ( (nvl(v_flag_recadastramento, 0) > 0) or (v_dat_recadastramento is not null) ) then
                    recenseamento.sp_registra_log_bd('PAC_RECADAST_NOVAPREV.FNC_PERM_CARGA_PESSOA_RECAD',
                                                      'COD_INS:'||i_cod_ins_vrb||', COD_IDE_CLI:'||i_cod_ide_cli_vrb,
                                                      'Recadastramento iniciado/realizado para beneficiario: flag_recadastramento='||
                                                            nvl(to_char(v_flag_recadastramento), '-nulo-') ||
                                                            ', dat_recadastramento='|| 
                                                            nvl(to_char(v_dat_recadastramento, 'dd/mm/yyyy hh24:mi'), '-nulo-') );
                    v_retorno := 'FALSE';
                    exit;
                end if;

            end loop;

            return (v_retorno);

        end;

        function veri_beneficio_encerrado(i_cod_ins_vbe in number,
                                          i_cod_ide_cli_vbe in varchar2) return varchar2 is

            v_ben_existe    boolean := false;
            v_ben_valido    boolean := false;
            v_retorno       varchar2(5) := 'TRUE';

        begin

            for bn in (select flg_status, dat_fim_ben
                       from user_ipesp.tb_beneficiario
                       where cod_ins         = i_cod_ins_vbe
                         and cod_ide_cli_ben = i_cod_ide_cli_vbe
                       ) loop 

                v_ben_existe := true;

                if ( ( bn.flg_status in ('A', 'S') ) and ( nvl(bn.dat_fim_ben, (sysdate + 1)) > sysdate ) ) then
                    v_ben_valido := true;
                    exit;
                end if;

            end loop;

            if (NOT v_ben_existe) then
                recenseamento.sp_registra_log_bd('PAC_RECADAST_NOVAPREV.FNC_PERM_CARGA_PESSOA_RECAD',
                                                  'COD_INS:'||i_cod_ins_vbe||', COD_IDE_CLI:'||i_cod_ide_cli_vbe,
                                                  'Pessoa fisica nao cadastrada em TB_BENEFICIARIO');
                v_retorno := 'FALSE';

            elsif (NOT v_ben_valido) then
                recenseamento.sp_registra_log_bd('PAC_RECADAST_NOVAPREV.FNC_PERM_CARGA_PESSOA_RECAD',
                                                  'COD_INS:'||i_cod_ins_vbe||', COD_IDE_CLI:'||i_cod_ide_cli_vbe,
                                                  'Status do beneficiario/data de fim do beneficio nao permitido para recenseamento');
                v_retorno := 'FALSE';

            end if;


            return (v_retorno);

        end;

    BEGIN

        if (   (i_cod_ins is null) 
            or (i_cod_ide_cli is null) 
            or (veri_recad_pessoa_fisica(i_cod_ins, i_cod_ide_cli) = 'FALSE') 
            or (veri_recad_beneficiario(i_cod_ins, i_cod_ide_cli) = 'FALSE') 
            or (veri_beneficio_encerrado(i_cod_ins, i_cod_ide_cli) = 'FALSE') 
            or (veri_dat_nasc_nula(i_cod_ins, i_cod_ide_cli) = 'FALSE')
            ) then

            return ('FALSE');

        end if;

        return ('TRUE');

    EXCEPTION
        WHEN OTHERS THEN    
            recenseamento.sp_registra_log_bd('PAC_RECADAST_NOVAPREV.FNC_PERM_CARGA_PESSOA_RECAD',
                                              'COD_INS:'||i_cod_ins||', COD_IDE_CLI:'||i_cod_ide_cli,
                                              sqlerrm);
            return ('FALSE');

    END FNC_PERM_CARGA_PESSOA_RECAD;            
    
    PROCEDURE sp_atu_pf_dat_obito(i_cod_ins     in tb_pessoa_fisica.cod_ins%type,
                                  i_cod_ide_cli in tb_pessoa_fisica.cod_ide_cli%type,
                                  o_cod_erro   out number,
                                  o_msg_erro   out varchar2)
    AS
      v_cod_erro number;
      v_msg_erro varchar2(2000);
      v_dat_obito_recens date;
      v_dat_obito_novapr date;
      v_id number;
      erro exception;
      v_flg_excluido char;
                                                      
    BEGIN
      -- Verificando se pessoa fisica existe
      begin 
        select pf.dat_obito, pf.id, pf.flg_excluido
          into v_dat_obito_recens, v_id, v_flg_excluido
          from tb_pessoa_fisica pf
         where pf.cod_ins = i_cod_ins
           and pf.cod_ide_cli = i_cod_ide_cli;   
      exception 
        when no_data_found then 
           v_cod_erro := 1;
           v_msg_erro := 'N?o localizada pessoa fisica '||i_cod_ide_cli||' na base do recenseamento';
           raise erro;
      end;
      --
      -- Se registro ja estiver excluido, n?o tem necessidade de fazer nada. 
      if nvl(v_flg_excluido,'N') = 'S' then 
        return;
      end if;
      --
      -- Verifica se existe data obito na novaprev
      begin 
        select pff.dat_obito
          into v_dat_obito_novapr
          from useR_ipesp.Tb_Pessoa_Fisica pff
         where pff.cod_ins = i_cod_ins
           and pff.cod_ide_cli = i_cod_ide_cli;
      exception 
        when no_data_found then 
          v_dat_obito_novapr := null;
          v_msg_erro := 'N?o localizada pessoa fisica '||i_cod_ide_cli||' na base da novaprev';
          raise erro;
      end;
      --
      --
      if v_dat_obito_recens is null and v_dat_obito_novapr is not null then 
        update tb_pessoa_fisica
           set dat_obito = v_dat_obito_novapr,
               flg_excluido = 'S',
               des_mot_excluido = 'Registro excluido por Obito no sistema da Novaprev.' ,
               dat_ult_atu = sysdate,
               nom_pro_ult_atu = 'sp_atu_pf_dat_obito'
         where id = v_id;
         -- apaga agendamento do cliente
         sp_remove_agendamento_pf(v_id,v_cod_erro,v_msg_erro);
         if v_cod_erro is not null then
           raise erro;
         end if;
      end if;
    exception 
      when erro then 
        o_cod_erro := v_cod_erro;
        o_msg_erro := '[PAC_RECADAST_NOVAPREV.SP_ATUALIZA_DAT_OBITO] - Erro Interno '||v_cod_erro||' - '||v_msg_erro;
      when others then 
        o_cod_erro := 0;
        o_msg_erro := '[PAC_RECADAST_NOVAPREV.SP_ATUALIZA_DAT_OBITO] - Erro Geral - '||sqlerrm;
    END sp_atu_pf_dat_obito;

    -----------------------------------------------------------------------------------------------
    
    function fnc_ret_rel_ben_pendente (i_nom_pessoa_fisica in varchar2,
                                       i_num_cpf           in varchar2,
                                       i_num_matricula     in varchar2, 
                                       i_mes_aniversario   in varchar2) return typ_tab_ben_pendente pipelined
    as
      v_rec_ben_pendente typ_rec_ben_pendente;
      
    begin
      for r in (
      SELECT DISTINCT P1.ID as ID,
                P1.COD_IDE_CLI as COD,
                P1.NUM_PRONTUARIO AS NUM_PRONTUARIO,
                P1.NOM_PESSOA_FISICA AS NOM_BENEFICIARIO,
                P1.CPF AS NUM_CPF,
                P1.TIPO_BENEFICIO AS TIPO_BENEFICIO,
                P1.DAT_CONCESSAO AS DAT_CONCESSAO,
                P1.TIP_LOGRADOURO AS TIP_LOGRADOURO,
                P1.LOGRADOURO AS LOGRADOURO,
                P1.NUMERO AS NUMERO,
                P1.BAIRRO AS BAIRRO,
                P1.MUNICIPIO AS MUNICIPIO,
                P1.CEP AS CEP,
                P1.ESTADO AS ESTADO,
                FNC_FORMATA_TELEFONE(P1.NUM_TEL_FONE1) AS TELEFONE,
                P1.DAT_NASC AS DAT_NASC,
                (CASE WHEN P1.flg_atend_ini_nao_finalizado = 'S' THEN '4'
                      WHEN P1.flg_agendou = 'S' AND P1.flg_comp_dia_agen = 'N' THEN '3'
                      WHEN P1.flg_agendou = 'S' THEN '2' 
                      ELSE '1' END) AS FLG_SIT_RECAD,
                P1.flg_comp_dia_agen,                
                P1.flg_atend_ini_nao_finalizado,
                P1.flg_agendou,                
                P2.NAME AS DES_SIT_RECAD                
        FROM (SELECT DISTINCT pf.id AS ID,
                              PF.COD_IDE_CLI AS COD_IDE_CLI,
                              PF.NUM_TEL_FONE1 AS NUM_TEL_FONE1,
                              CAST(PF.dat_nasc AS DATE) AS NASC,
                              pf.nom_pessoa_fisica AS NOM_PESSOA_FISICA,
                              pf.num_cpf AS CPF,
                              BN.NUM_PRONTUARIO AS NUM_PRONTUARIO,
                              TB.name AS TIPO_BENEFICIO,
                              CAST(BN.dat_concessao AS DATE) AS DAT_CONCESSAO,
                              T1.NOM_TIPO AS TIP_LOGRADOURO,
                              E1.NOM_LOGRADOURO AS LOGRADOURO,
                              E1.NUM_NUMERO AS NUMERO,
                              B1.NOM_BAIRRO AS BAIRRO,
                              M1.NOM_ABREVIADO AS MUNICIPIO,
                              E1.NUM_CEP AS CEP,
                              E2.COD_DOMAIN_DETAIL AS ESTADO,
                              PF.DAT_NASC AS DAT_NASC,
                              nvl((SELECT distinct 'S' FROM tb_agendamento ag1 WHERE ag1.pessoa_fisica_id = pf.id),'N') AS flg_agendou,
                              nvl((SELECT distinct 'S' FROM tb_agendamento ag1 
                                    WHERE ag1.pessoa_fisica_id = pf.id 
                                      AND ag1.dat_agendamento < SYSDATE
                                      AND EXISTS (SELECT 1
                                                    FROM tb_atendimento at1
                                                   WHERE at1.servidor_id = pf.id
                                                     AND TO_DATE(TO_CHAR(at1.dat_ini_atend,'DD/MM/YYYY') || '00:00','DD/MM/YYYY HH24:MI') =
                                                         TO_DATE(TO_CHAR(ag1.dat_agendamento,'DD/MM/YYYY') ||'00:00','DD/MM/YYYY HH24:MI'))
                                   ),'N') AS flg_comp_dia_agen,
                              nvl((SELECT DISTINCT 'S'
                                    FROM tb_atendimento at1
                                   WHERE at1.servidor_id = pf.id
                                     AND NOT EXISTS (SELECT 1
                                                       FROM tb_protocolo pr
                                                      WHERE pr.cod_ide_cli = pf.cod_ide_cli)
                                   ),'N') AS flg_atend_ini_nao_finalizado
                FROM TB_PESSOA_FISICA PF
                  LEFT JOIN TB_END_PESSOA_FISICA E1
                    ON (PF.COD_IDE_CLI = E1.COD_IDE_CLI AND E1.COD_TIPO_END = 601)
                  LEFT JOIN TB_END_TIPO_LOGRADOURO T1
                    ON (T1.ID = E1.COD_TIP_LOGRADOURO)
                  LEFT JOIN TB_END_BAIRRO B1
                    ON (E1.COD_BAIRRO = B1.ID)
                  LEFT JOIN TB_END_LOCALIDADE M1
                    ON (E1.COD_MUNICIPIO = M1.ID)
                  LEFT JOIN TB_DOMINIO_DETALHE E2
                    ON (E2.DOMAIN_ID = 3 AND E2.ID = E1.COD_UF)
                  LEFT JOIN tb_usuario us
                    ON us.pessoa_fisica_id = pf.id
                  LEFT JOIN tb_usuario_perfil up
                    ON up.usuario_id = us.id
                  LEFT JOIN tb_perfil pe
                    ON pe.id = up.perfil_id
                  LEFT JOIN tb_beneficiario bn
                    ON bn.cod_ide_cli = pf.cod_ide_cli
                  LEFT JOIN tb_dominio_detalhe tb
                    ON tb.domain_id = (SELECT id FROM tb_dominio WHERE cod_domain = 'TIPO_BENEFICIO')
                    AND tb.cod_domain_detail = bn.cod_tipo_beneficio        
                  LEFT JOIN tb_protocolo pr
                    ON pr.cod_ide_cli = pf.cod_ide_cli
               WHERE pe.authority = 'ROLE_USER'
                 AND PF.Dat_Obito is null
                 AND pr.id IS NULL
                 AND ((i_nom_pessoa_fisica is not null AND upper(pf.nom_pessoa_fisica) LIKE upper('%' || i_nom_pessoa_fisica || '%')) OR (i_nom_pessoa_fisica is null))
                 AND ((i_num_cpf is not null AND pf.num_cpf LIKE upper('%' || i_num_cpf || '%')) OR (i_num_cpf is null ))
                 AND ((i_num_matricula is not null AND bn.num_prontuario LIKE upper('%' || i_num_matricula || '%')) OR (i_num_matricula is null AND 1 = 1))
                 AND ((i_mes_aniversario is not null AND TO_CHAR(pf.dat_nasc, 'MM') LIKE upper('%' || i_mes_aniversario || '%')) OR (i_mes_aniversario is null AND 1 = 1))
               ORDER BY pf.nom_pessoa_fisica
             ) P1
        LEFT JOIN tb_dominio_detalhe P2 ON P2.domain_id = (SELECT id FROM tb_dominio WHERE cod_domain = 'SITUACAO_RECADASTRAMENTO')
                                        AND P2.cod_domain_detail = (CASE WHEN P1.flg_atend_ini_nao_finalizado = 'S' THEN '4'
                                                                         WHEN P1.flg_agendou = 'S' AND P1.flg_comp_dia_agen = 'N' THEN '3'
                                                                         WHEN P1.flg_agendou = 'S' THEN '2'
                                                                         ELSE '1' END)
      ) 
      loop 
        v_rec_ben_pendente.cod                          :=  r.cod;                       
        v_rec_ben_pendente.id                           :=  r.id;        
        v_rec_ben_pendente.num_prontuario               :=  r.num_prontuario;
        v_rec_ben_pendente.nom_beneficiario             :=  r.nom_beneficiario;
        v_rec_ben_pendente.num_cpf                      :=  r.num_cpf;
        v_rec_ben_pendente.tipo_beneficio               :=  r.tipo_beneficio;
        v_rec_ben_pendente.dat_concessao                :=  r.dat_concessao; 
        v_rec_ben_pendente.tip_logradouro               :=  r.tip_logradouro;
        v_rec_ben_pendente.logradouro                   :=  r.logradouro;    
        v_rec_ben_pendente.numero                       :=  r.numero;        
        v_rec_ben_pendente.bairro                       :=  r.bairro;        
        v_rec_ben_pendente.municipio                    :=  r.municipio;     
        v_rec_ben_pendente.cep                          :=  r.cep;           
        v_rec_ben_pendente.estado                       :=  r.estado;        
        v_rec_ben_pendente.telefone                     :=  r.telefone;      
        v_rec_ben_pendente.dat_nasc                     :=  r.dat_nasc;      
        v_rec_ben_pendente.flg_sit_recad                :=  r.flg_sit_recad; 
        v_rec_ben_pendente.des_sit_recad                :=  r.des_sit_recad; 
        v_rec_ben_pendente.flg_comp_dia_agen            := r.flg_comp_dia_agen;
        v_rec_ben_pendente.flg_agendou                  := r.flg_agendou;
        v_rec_ben_pendente.flg_atend_ini_nao_finalizado := r.flg_atend_ini_nao_finalizado;
        
        pipe row(v_rec_ben_pendente);
        
      end loop; 
      
    end fnc_ret_rel_ben_pendente;     
    --
    PROCEDURE sp_atu_pf_ben_2019(i_cod_ins     in tb_pessoa_fisica.cod_ins%type,
                                    i_cod_ide_cli in tb_pessoa_fisica.cod_ide_cli%type,
                                    o_cod_erro   out number,
                                    o_msg_erro   out varchar2)
    as
      v_id number;
      v_qtd_ben number;
      v_flg_excluido char;
    begin
      --
      select count(1)
        into v_qtd_ben
        from user_ipesp.tb_pessoa_fisica pf
       where pf.cod_ide_cli = i_cod_ide_cli
         and exists( select '1' 
                       from user_ipesp.tb_beneficiario b 
                      where b.cod_ide_cli_ben = pf.cod_ide_cli
                        and b.dat_ini_ben >= trunc(sysdate,'yyyy')
                        and b.dat_fim_ben is null 
                        and b.flg_status in ('A','V','S'))
         and not exists( select '1' 
                       from user_ipesp.tb_beneficiario b 
                      where b.cod_ide_cli_ben = pf.cod_ide_cli
                        and b.dat_ini_ben < trunc(sysdate,'yyyy'));                                                                                        
      --                            
      if v_qtd_ben > 0 then 
        --
        select id, flg_excluido
          into v_id, v_flg_excluido
          from tb_pessoa_fisica 
         where cod_ins = i_cod_ins
           and cod_ide_cli = i_cod_ide_cli;  
        --
        if nvl(v_flg_excluido,'N') <> 'S' then 
          update tb_pessoa_fisica pf
             set pf.flg_excluido = 'S',
                 pf.des_mot_excluido = 'Beneficio iniciou apos 2019 na novaprev para o servidor.',
                 dat_ult_atu = sysdate,
                 nom_pro_ult_atu = 'sp_atu_pf_ben_2019'
           where pf.cod_ins = i_cod_ins
             and pf.cod_ide_cli = i_cod_ide_cli;                 
          --
          sp_remove_agendamento_pf(v_id,o_cod_erro, o_msg_erro);
          --
        end if;
      end if;                                    
    end sp_atu_pf_ben_2019;
    
      
     PROCEDURE sp_atu_pf_ben_cessado(i_cod_ins     in tb_pessoa_fisica.cod_ins%type,
                                    i_cod_ide_cli in tb_pessoa_fisica.cod_ide_cli%type,
                                    o_cod_erro   out number,
                                    o_msg_erro   out varchar2)
    as
      v_id number;
      v_qtd_ben number;
      v_flg_excluido char;
    begin
      --
      select count(1)
        into v_qtd_ben
        from user_ipesp.tb_beneficiario ben 
       where ben.cod_ins = i_cod_ins
         and ben.cod_ide_cli_ben = i_cod_ide_cli
         and ben.flg_status not in ('V','A','S')
         and not exists (select '1'
                           from user_ipesp.tb_beneficiario ben
                          where ben.cod_ins = i_cod_ins
                            and ben.cod_ide_cli_ben = i_cod_ide_cli
                            and ben.flg_status in ('V','A','S'));                                                                                                
      --                            
      if v_qtd_ben > 0 then 
        --
        select id, flg_excluido
          into v_id, v_flg_excluido
          from tb_pessoa_fisica 
         where cod_ins = i_cod_ins
           and cod_ide_cli = i_cod_ide_cli;  
        --
        if nvl(v_flg_excluido,'N') <> 'S' then 
          update tb_pessoa_fisica pf
             set pf.flg_excluido = 'S',
                 pf.des_mot_excluido = 'Beneficio cessado na novaprev para o servidor.',
                 dat_ult_atu = sysdate,
                 nom_pro_ult_atu = 'sp_atu_pf_ben_cessado'
           where pf.cod_ins = i_cod_ins
             and pf.cod_ide_cli = i_cod_ide_cli;                 
          --
          sp_remove_agendamento_pf(v_id,o_cod_erro, o_msg_erro);
          --
        end if;
      end if;                                    
    end sp_atu_pf_ben_cessado;                                                                           
    --
    
    --
    -- Rotina que remove  todos os agendamentos de uma pessoa fisica
    procedure sp_remove_agendamento_pf(i_id_pf    in  tb_pessoa_fisica.id%type,
                                       o_cod_erro out number,
                                       o_msg_erro out varchar2)
    as      
    begin 
      insert into tb_agendamento_historico
      select id,
             cod_entidade,
             dat_agendamento,
             dat_ger_prot,
             hora_final,
             hora_inicio,
             local_id,
             pessoa_fisica_id,
             regiao_id,
             tipo,
             motivo_alteracao,
             flg_recadastramento,
             dat_recadastramento,
             flg_recenseamento,
             dat_recenseamento,
             sysdate,
             sysdate,
             'pac_recadast_novaprev.sp_remove_agendamento',
             'pac_recadast_novaprev.sp_remove_agendamento',
             'DELETE'
      from recenseamento.tb_agendamento a
      where a.pessoa_fisica_id = i_id_pf
        and a.dat_agendamento >= sysdate;      
      --
      delete tb_agendamento a
       where a.pessoa_fisica_id = i_id_pf
         and a.dat_agendamento >= sysdate;
      --  
    exception 
      when others then 
        o_cod_erro := 1;                            
        o_msg_erro := '[PAC_RECADAST_NOVAPREV.SP_REMOVE_AGENDAMENTO_PF] - '||sqlerrm;
    end sp_remove_agendamento_pf;
    --
    --
  procedure sp_login_atu_cadastro(i_cod_ins     in tb_pessoa_Fisica.cod_ins%type,
                                  i_cpf_login   in tb_pessoa_Fisica.num_cpf%type,
                                  i_cod_ide_cli in tb_pessoa_Fisica.cod_ide_cli%type,
                                  o_msg_erro    out varchar2 )
  as
    v_cod_ide_cli tb_pessoa_Fisica.cod_ide_cli%type;
    v_msg varchar2(4000);
  begin 
    if i_cod_ide_cli is null then           
      select cod_ide_cli
        into v_cod_ide_cli
        from user_ipesp.tb_pessoa_fisica          
        where cod_ins ='1';
    else
      v_cod_ide_cli := i_cod_ide_cli;
    end if;
      
    SP_CARGA_DADOS_RECADAST(i_cod_ins     => i_cod_ins,
                            i_cod_ide_cli => v_cod_ide_cli,
                            o_msg         => v_msg );
    --
    o_msg_erro := v_msg;
    commit;    
                     
  exception 
    when others then 
      v_msg := sqlerrm;
      rollback;
      o_msg_erro := v_msg;                    
  end sp_login_atu_cadastro;
    
END PAC_RECADAST_NOVAPREV;
/

