create or replace procedure sp_fecha_periodo_censo(i_id_censo in tb_censo.id%type,
                                                   i_nom_usu  in varchar2,
                                                   o_cod_erro out number,
                                                   o_des_erro out varchar2)
as
  v_id_his_per_censo number;
begin
   update tb_censo c
      set c.flg_status = 'F', 
          c.nom_usu_ult_atu = i_nom_usu,
          c.dat_fechamento = sysdate,
          c.dat_ult_atu = sysdate,
          c.usu_fechamento =  i_nom_usu    
    where c.id = i_id_censo 
      and flg_status <> 'F' ;
  
   
  v_id_his_per_censo := seq_hist_periodo_censo.nextval;
  
  
 /*

  insert into tb_hist_pessoa_fisica
       select a.*,i_id_censo
         from tb_pessoa_fisica a;

  delete tb_pessoa_fisica;

  insert into tb_hist_end_pessoa_fisica
       select a.*, i_id_censo
         from tb_end_pessoa_fisica a;

  delete tb_end_pessoa_fisica;


  insert into tb_hist_dependente
       select a.*, i_id_censo
         from tb_dependente a;

  delete tb_dependente;

  insert into tb_hist_representante_legal
       select a.*, i_id_censo
         from tb_representante_legal a;*/

  delete tb_representante_legal;

   

exception 
  when others then 
    o_cod_erro := sqlcode;
    o_des_erro := sqlerrm;  
end sp_fecha_periodo_censo;
/

