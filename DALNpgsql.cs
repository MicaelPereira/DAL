﻿//using System;
//using System.Collections.Generic;
//using System.Linq;
//using System.Text;
//using Npgsql;
//using System.Data;

//namespace MyTip.Lib.Util.UtilDAL
//{
//    public class DALNpgsql
//    {
//        #region INFO: Atributos

//        private NpgsqlConnection _conexao;
//        private string _stringConexao;
//        private NpgsqlTransaction objTrans;

//        #endregion

//        #region INFO: Construtor

//        /// <summary>
//        /// Método construtor da classe
//        /// </summary>
//        public DALNpgsql()
//        {
//            GC.Collect();
//            string serverName = "177.67.82.170", //Alterar para localhost.
//                port = "5432", 
//                userName = "postgres", 
//                password = "mytip2013", 
//                databaseName = "mytip";
//            _stringConexao = String.Format("Server={0};Port={1};User Id={2};Password={3};Database={4};", serverName, port, userName, password, databaseName);
//            //"Data Source=177.67.82.170;Initial Catalog=MCPNET;User ID=sa;Password=intranet2000; connection timeout=99999;";
//            _conexao = new NpgsqlConnection(_stringConexao);
//        }
        
//        #endregion

//        #region INFO: Métodos

//        /// <summary>
//        /// Método responsável pela conexão à fonte de dados
//        /// </summary>
//        public void Conectar()
//        {

//            if (ConexaoAtiva())
//                _conexao.Dispose();

//            _conexao.Open();

//        }

//        /// <summary>
//        /// Método responsável por verificar se a conexão está ativa.
//        /// </summary>
//        public bool ConexaoAtiva()
//        {

//            switch (_conexao.State)
//            {

//                case ConnectionState.Connecting:
//                case ConnectionState.Broken:
//                case ConnectionState.Executing:
//                case ConnectionState.Fetching:
//                case ConnectionState.Open:
//                    return true;

//                case ConnectionState.Closed:
//                    return false;

//                default:
//                    return false;

//            }

//        }

//        /// <summary>
//        /// Método responsável por desconectar-se da fonte de dados
//        /// </summary>
//        public void Desconectar()
//        {

//            if (ConexaoAtiva())
//            {
//                _conexao.Close();
//                _conexao.Dispose();
//            }

//        }

//        /// <summary>
//        /// Método responsável pela execução de consultas de ação (que inserem, modificam e deletam dados)
//        /// </summary>
//        /// <param name="comando">SqlCommand que contém a consulta a ser executada</param>
//        /// <returns>Um número inteiro que indica a quantidade de linhas afetadas pela consulta</returns>
//        /// <remarks></remarks>
//        public int Executar(NpgsqlCommand comando)
//        {
//            int linhasAfetadas = 0;

//            try
//            {
//                Conectar();
//                objTrans = _conexao.BeginTransaction();
//                comando.Transaction = objTrans;
//                comando.Connection = _conexao;
//                linhasAfetadas = comando.ExecuteNonQuery();
//                objTrans.Commit();
//            }
//            catch (Exception)
//            {
//                objTrans.Rollback();
//                linhasAfetadas = -1;
//            }
//            finally
//            {
//                objTrans.Dispose();
//                Desconectar();
//            }
//            return linhasAfetadas;
//        }

//        /// <summary>
//        /// Método responsável pela execução de consultas de seleção (que retornam dados)
//        /// </summary>
//        /// <param name="comando">SqlCommand que contém a consulta a ser executada</param>
//        /// <returns>DataSet contendo as linhas retornadas pela consulta</returns>
//        /// <remarks></remarks>
//        public DataSet Consultar(NpgsqlCommand comando)
//        {
//            DataSet ds = new DataSet();
//            NpgsqlDataAdapter da = new NpgsqlDataAdapter();

//            try
//            {

//                ds.Tables.Add("tabela");

//                // A primeira delas foi definir a propriedade - EnforceConstraints - como false;
//                // isto desabilita a verificação das restrições durante a operação e pode tornar a operação mais rápida.
//                //Você pode voltar a definir o valor como True depois que os dados forem retornados dentro um loop try/Catch e tratando a exceção ConstraintException.
//                ds.EnforceConstraints = false;

//                da.SelectCommand = comando;
//                comando.Connection = _conexao;

//                Conectar();

//                //Outro fator que onera o desempenho de um DataSet é o estabelecimento de uma chave primária. Podemos também desabilitar temporariamente a indexação e notificação interna. Para isto fazemos o seguinte:
//                //1- Executamos o método BeginLoadData antes de usar o método Fill para desabilitar a notificação , indexação.
//                //2- Executamos o método EndLoadData depois de usar o método Fill para habilitar a indexação a notificação.
//                //Estes métodos são membros da classe DataTable e por isso você vai precisar chamá-los para a DataTable particular que você esta preenchendo.
//                //Com isto melhoramos o desempenho do DataSet e com isto justificamos sua utilização afim de podermos usar seus recursos.
//                ds.Tables["tabela"].BeginLoadData();
//                da.Fill(ds, "tabela");
//                ds.Tables["tabela"].EndLoadData();
//            }
//            catch (NpgsqlException err)
//            {
//                #region INFO: SqlException
//                throw err;
//                //ex.Message.ToString() / ex.Server.ToString() / ex.Procedure.ToString() / ex.LineNumber.ToString() / ex.ErrorCode.ToString()
//                //ex.Source.ToString() / ex.Errors.ToString() / ex.State.ToString() / ex.ToString() / comando.CommandText.ToString() / comando.CommandType.ToString())
//                #endregion
//            }
//            finally
//            {
//                //depois colocar para enviar email.
//                Desconectar();
//            }
//            return ds;
//        }

//        #endregion
//    }
//}
