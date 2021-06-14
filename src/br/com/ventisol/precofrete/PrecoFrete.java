/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package br.com.ventisol.precofrete;

import br.com.sankhya.extensions.actionbutton.AcaoRotinaJava;
import br.com.sankhya.extensions.actionbutton.ContextoAcao;
import br.com.sankhya.extensions.actionbutton.Registro;
import br.com.sankhya.jape.EntityFacade;
import br.com.sankhya.jape.core.JapeSession;
import br.com.sankhya.jape.dao.JdbcWrapper;
import br.com.sankhya.jape.sql.NativeSql;
import br.com.sankhya.modelcore.util.EntityFacadeFactory;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.Charset;
import java.sql.CallableStatement;
import java.sql.ResultSet;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

public class PrecoFrete implements AcaoRotinaJava{

    EntityFacade dwFacade = EntityFacadeFactory.getDWFFacade();
    JapeSession.SessionHandle hnd = null;

    @Override
    public void doAction(ContextoAcao contexto) throws Exception {

        JdbcWrapper jdbc = null;

        try {

            jdbc = dwFacade.getJdbcWrapper();
            NativeSql qryPEDIDO = new NativeSql(jdbc);
            int qtdeRS;
            

            for (Registro registro : contexto.getLinhas()) {

                // Pegando o paramentro da tela
                BigDecimal nunota = (BigDecimal) registro.getCampo("NUNOTA");
                BigDecimal numnota = (BigDecimal) registro.getCampo("NUMNOTA");
                BigDecimal ordemcarga = (BigDecimal) registro.getCampo("ORDEMCARGA");

                //verificando se o pedido já foi desmembrado
                qryPEDIDO.setNamedParameter("NUNOTA", nunota);
                ResultSet rsPEDIDO = qryPEDIDO.executeQuery("SELECT JSONTEXT FROM AD_VW_DATAFRETE_JSON WHERE NUNOTA = :NUNOTA");

                qtdeRS = 0;
                String retornoAPI = "";
                while (rsPEDIDO.next()) {
                    retornoAPI = rsPEDIDO.getString("JSONTEXT");
                    qtdeRS++;
                }

                if (qtdeRS == 0) {
                    throw new Exception("Pedido " + nunota + " não localizado!");
                }else{

                    //apagando os dados de cotação existente
                    try {
                        jdbc.openSession();
                        NativeSql qryExcluir = new NativeSql(jdbc);
                        qryExcluir.setNamedParameter("NUNOTA", nunota);
                        qryExcluir.setNamedParameter("NUMNOTA", numnota);
                        qryExcluir.setNamedParameter("ORDEMCARGA", ordemcarga);
                        boolean rsExcluir = qryExcluir.executeUpdate("DELETE FROM AD_DTFRETECOTACAO WHERE NUNOTA = :NUNOTA and NUMNOTA = :NUMNOTA and ORDEMCARGA = :ORDEMCARGA");
                        jdbc.closeSession();
                    } catch (Exception e) {
                        throw new Exception("Erro:  " + e.getMessage());
                    }
                    
                    
                    //processando atualização de precos
                    try {

                        //conectando no servidor do DataFrete    
                        URL url = new URL("https://ventisol.api.datafreteapi.com");
                        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
                        conn.setDoOutput(true);
                        conn.setRequestMethod("POST");
                        conn.setRequestProperty("Content-Type", "application/json; utf-8");
                        conn.setRequestProperty("Accept", "application/json");
//                        conn.setRequestProperty("Content-Type", "application/json; charset=utf-8");

                        //Montando o Json com os dados do pedido para enviar para o DataFrete
                        String input = retornoAPI;
                        OutputStream os = conn.getOutputStream();
                        os.write(input.getBytes());
                        os.flush();

                        //Lendo o Retorno do DataFrete
                        BufferedReader br = new BufferedReader(new InputStreamReader(conn.getInputStream(), Charset.forName("UTF-8")));
                        int output;
                        StringBuilder stringJson = new StringBuilder();
                        while ((output = br.read()) != -1) {
                            stringJson.append((char)output);
                        }
                        conn.disconnect();

                        //verificando se no retorno tem os dados dos calculos e decompondo a informação
                        try {
                            JSONObject jsonObject = new JSONObject(stringJson.toString());                            
                            if(jsonObject.get("codigo_retorno").toString().equals("1")){

                                String vDataFrete_id_cotacao;
                                String vDataFrete_tp_entrega;   
                                String vDataFrete_nome_transportador;   // NOME DO TRANSPORTADOR STRING
                                String vDataFrete_cnpj_transportador;   // DOCUMENTO DO TRANSPORTADOR STRING
                                String vDataFrete_cod_transportador;    // CÓDIGO DO TRANSPORTADOR STRING
                                String vDataFrete_descricao;            // DESCRIÇÃO DA TABELA DE FRETE STRING
                                String vDataFrete_valor_frete;          // VALOR REAL DO FRETE SIMULADO (R$) FLOAT (10,2)
                                String vDataFrete_valor_frete_exibicao; // VALOR DO FRETE SIMULADO COM CAMPANHA DE FRETE (R$) FLOAT (10,2)
                                String vDataFrete_valor_icms;           // VALOR DO ICMS APLICADO (R$) FLOAT (10,2)
                                String vDataFrete_aliquota_icms;        // ALIQUOTA DE ICMS APLICADA (%) FLOAT (2,2)
                                String vDataFrete_prazo;                // PRAZO DE ENTREGA (D) INT
                                String vDataFrete_prazo_exibicao;       // PRAZO DE ENTREGA COM CAMPANHA DE FRETE (D) INT
                                String vDataFrete_cod_tabela;           // CÓDIGO DA TABELA DE FRETE NO SISTEMA INT
                                String vDataFrete_data_simulacao;       // DATA DA SIMULAÇÃO (AAAA-MM-DD) DATE                            
                                String vDataFrete_hora_simulacao;       // HORA DA SIMULAÇÃO (HH:MM:SS) TIME                    

                                JSONArray jsonItens = new JSONArray(jsonObject.get("data").toString());

                                vDataFrete_id_cotacao = jsonObject.get("id_cotacao").toString();
                                for (int i = 0; i < jsonItens.length(); i++) {
                                    JSONObject jsonItem = jsonItens.getJSONObject(i);  
                                    vDataFrete_tp_entrega = jsonItem.get("tp_entrega").toString();
                                    vDataFrete_nome_transportador = jsonItem.get("nome_transportador").toString();
                                    vDataFrete_cnpj_transportador = jsonItem.get("cnpj_transportador").toString();
                                    vDataFrete_cod_transportador = jsonItem.get("cod_transportador").toString();
                                    vDataFrete_descricao = jsonItem.get("descricao").toString();
                                    vDataFrete_valor_frete = jsonItem.get("valor_frete").toString();
                                    vDataFrete_valor_frete_exibicao = jsonItem.get("valor_frete_exibicao").toString();
                                    vDataFrete_valor_icms = jsonItem.get("valor_icms").toString();
                                    vDataFrete_aliquota_icms = jsonItem.get("aliquota_icms").toString();
                                    vDataFrete_prazo = jsonItem.get("prazo").toString();
                                    vDataFrete_prazo_exibicao = jsonItem.get("prazo_exibicao").toString();
                                    vDataFrete_cod_tabela = jsonItem.get("cod_tabela").toString();
                                    vDataFrete_data_simulacao = jsonItem.get("data_simulacao").toString();
                                    vDataFrete_hora_simulacao = jsonItem.get("hora_simulacao").toString();

                                    //salvando os dados de cotação
                                    try {
                                        jdbc.openSession();
                                        NativeSql qryIncluir = new NativeSql(jdbc);
                                        qryIncluir.setNamedParameter("NUNOTA", nunota);
                                        qryIncluir.setNamedParameter("NUMNOTA", numnota);
                                        qryIncluir.setNamedParameter("ORDEMCARGA", ordemcarga);
                                        qryIncluir.setNamedParameter("REGISTRO", i);
                                        qryIncluir.setNamedParameter("TIPOENTREGA", vDataFrete_tp_entrega);
                                        qryIncluir.setNamedParameter("NOME_TRANSPORTADO", vDataFrete_nome_transportador);
                                        qryIncluir.setNamedParameter("CNPJ_TRANSPORTADOR", vDataFrete_cnpj_transportador);
                                        qryIncluir.setNamedParameter("COD_TRANSPORTADOR", vDataFrete_cod_transportador);
                                        qryIncluir.setNamedParameter("DESCRICAO", vDataFrete_descricao);
                                        qryIncluir.setNamedParameter("VALOR_FRETE_EXIBICAO", vDataFrete_valor_frete_exibicao.replace(".", ","));
                                        qryIncluir.setNamedParameter("VALOR_FRETE", vDataFrete_valor_frete.replace(".", ","));
                                        qryIncluir.setNamedParameter("VALOR_ICMS", vDataFrete_valor_icms.replace(".", ","));
                                        qryIncluir.setNamedParameter("ALIQUOTA_ICMS", vDataFrete_aliquota_icms.replace(".", ","));
                                        qryIncluir.setNamedParameter("PRAZO_EXIBICAO", vDataFrete_prazo_exibicao);
                                        qryIncluir.setNamedParameter("PRAZO", vDataFrete_prazo);
                                        qryIncluir.setNamedParameter("DATA_SIMULACAO", vDataFrete_data_simulacao.split("-")[2] + "/" + vDataFrete_data_simulacao.split("-")[1] + "/" + vDataFrete_data_simulacao.split("-")[0]);
                                        qryIncluir.setNamedParameter("HORA_SIMULACAO", vDataFrete_hora_simulacao);
                                        qryIncluir.setNamedParameter("COD_TABELA", vDataFrete_cod_tabela);
                                        qryIncluir.setNamedParameter("ID_COTACAO", vDataFrete_id_cotacao);
                                        
                                        boolean rsIncluir = qryIncluir.executeUpdate("INSERT INTO AD_DTFRETECOTACAO (ORDEMCARGA, NUMNOTA, NUNOTA, REGISTRO, TIPOENTREGA, NOME_TRANSPORTADO, CNPJ_TRANSPORTADOR, COD_TRANSPORTADOR, DESCRICAO, VALOR_FRETE_EXIBICAO, VALOR_FRETE, VALOR_ICMS, ALIQUOTA_ICMS, PRAZO_EXIBICAO, PRAZO, DATA_SIMULACAO, HORA_SIMULACAO, COD_TABELA, ID_COTACAO)VALUES(:ORDEMCARGA, :NUMNOTA, :NUNOTA, :REGISTRO, :TIPOENTREGA, :NOME_TRANSPORTADO, :CNPJ_TRANSPORTADOR, :COD_TRANSPORTADOR, :DESCRICAO, :VALOR_FRETE_EXIBICAO, :VALOR_FRETE, :VALOR_ICMS, :ALIQUOTA_ICMS, :PRAZO_EXIBICAO, :PRAZO, :DATA_SIMULACAO, :HORA_SIMULACAO, :COD_TABELA, :ID_COTACAO)");
                                        //contexto.setMensagemRetorno(vDataFrete_hora_simulacao);
                                        jdbc.closeSession();
                                    } catch (Exception e) {
                                        throw new Exception("Erro:  " + e.getMessage());
                                    }

                                }
                            }
                        } catch (Exception e) {
                            throw new Exception("Erro:  " + e.getMessage());
                        }

                    } catch (MalformedURLException e) {
                        throw new Exception("Erro:  " + e.getMessage());
                    } catch (IOException e) {
                        throw new Exception("Erro:  " + e.getMessage());
                    }                     

                    
                }

                rsPEDIDO.close();
                contexto.setMensagemRetorno("Preços importados com sucesso");

            }
            
        } finally {
            //Finalizando ação e fechando conexoes
            JapeSession.close(hnd);
            JdbcWrapper.closeSession(jdbc);
        }
    }
    
}
