using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Threading.Tasks;

namespace DataAccessLayer {
	public class MySqlDatabase {
		private readonly string _connectionString;
		private const string _className = "MySqlDatabase";
		private static DateTime _lastConnectionFailedLog;

		public MySqlDatabase(string host = null, string user = null, string password = null, string database = null) {
			host ??= Environment.GetEnvironmentVariable("MYSQL_SERVER");
			user ??= Environment.GetEnvironmentVariable("MYSQL_USER");
			password ??= Environment.GetEnvironmentVariable("MYSQL_PASS");
			database ??= Environment.GetEnvironmentVariable("MYSQL_DATA");
			string[] splitHost = host.Split(":");

			if (splitHost.Length == 2)
				host = splitHost[0] + ";port=" + splitHost[1];

			_connectionString = $"server={host};database={database};user={user};password={password};convert zero datetime = True";
		}

		public async Task<DataTable> Select(string query, params object[] parameters) {
			try {
				using MySqlConnection conn = new MySqlConnection(_connectionString);
				using MySqlCommand cmd = new MySqlCommand(query, conn);

				await conn.OpenAsync();

				MySqlDataAdapter adapter = new MySqlDataAdapter(cmd);

				if (parameters.Length > 0) {
					string[] parametersKeys = GetParameters(query);
					if (parametersKeys.Length != parameters.Length) {
						throw new ArgumentException("Parameters keys length don't match with parameters values length. Check query.");
					}
					for (int i = 0; i < parameters.Length; i++) {
						cmd.Parameters.AddWithValue(parametersKeys[i], ConvertObject(parameters[i]));
					}
				} else if (GetParameters(query).Length != 0) {
					throw new ArgumentException("Parameters keys length don't match with parameters values length. Check query.");
				}

				DataTable data = new DataTable();
				await adapter.FillAsync(data);

				conn.Close();

				return data;
			} catch (Exception e) {
				LogException(e, query, parameters);

				return null;
			}
		}

		public async Task<int> Insert(string query, params object[] parameters) {
			try {
				using MySqlConnection conn = new MySqlConnection(_connectionString);
				using MySqlCommand cmd = new MySqlCommand(query, conn);

				await conn.OpenAsync();

				if (parameters.Length > 0) {
					string[] parametersKeys = GetParameters(query);
					if (parametersKeys.Length != parameters.Length) {
						throw new ArgumentException("Parameters keys length don't match with parameters values length. Check query.");
					}
					for (int i = 0; i < parameters.Length; i++) {
						cmd.Parameters.AddWithValue(parametersKeys[i], ConvertObject(parameters[i]));
					}
				} else if (GetParameters(query).Length != 0) {
					throw new ArgumentException("Parameters keys length don't match with parameters values length. Check query.");
				}

				await cmd.ExecuteNonQueryAsync();
				conn.Close();

				return (int)cmd.LastInsertedId;
			} catch (Exception e) {
				LogException(e, query, parameters);

				return -1;
			}
		}

		public async Task<bool> Update(string query, params object[] parameters) {
			try {
				using MySqlConnection conn = new MySqlConnection(_connectionString);
				using MySqlCommand cmd = new MySqlCommand(query, conn);

				await conn.OpenAsync();

				if (parameters.Length > 0) {
					string[] parametersKeys = GetParameters(query);
					if (parametersKeys.Length != parameters.Length) {
						throw new ArgumentException("Parameters keys length don't match with parameters values length. Check query.");
					}
					for (int i = 0; i < parameters.Length; i++) {
						cmd.Parameters.AddWithValue(parametersKeys[i], ConvertObject(parameters[i]));
					}
				} else if (GetParameters(query).Length != 0) {
					throw new ArgumentException("Parameters keys length don't match with parameters values length. Check query.");
				}

				await cmd.ExecuteNonQueryAsync();
				conn.Close();

				return true;
			} catch (Exception e) {
				LogException(e, query, parameters);

				return false;
			}
		}

		public async Task<bool> Delete(string query, params object[] parameters) {
			try {
				using MySqlConnection conn = new MySqlConnection(_connectionString);
				using MySqlCommand cmd = new MySqlCommand(query, conn);
				
				await conn.OpenAsync();

				if (parameters.Length > 0) {
					string[] parametersKeys = GetParameters(query);
					if (parametersKeys.Length != parameters.Length) {
						throw new ArgumentException("Parameters keys length don't match with parameters values length. Check query.");
					}
					for (int i = 0; i < parameters.Length; i++) {
						cmd.Parameters.AddWithValue(parametersKeys[i], ConvertObject(parameters[i]));
					}
				} else if (GetParameters(query).Length != 0) {
					throw new ArgumentException("Parameters keys length don't match with parameters values length. Check query.");
				}

				await cmd.ExecuteNonQueryAsync();
				conn.Close();

				return true;
			} catch (Exception e) {
				LogException(e, query, parameters);

				return false;
			}
		}

		private static string[] GetParameters(string sql) {
			char[] limitingChars = { ' ', ')', ',' };
			List<string> parameters = new List<string>();
			for (int i = 0; i < sql.Length; i++) {
				if (sql[i] == '@') {
					int index = -1;

					for (int j = 0; j < limitingChars.Length; j++) {
						int indexLimit = sql.IndexOf(limitingChars[j], i);
						if ((indexLimit < index && indexLimit != -1) || index == -1) {
							index = indexLimit;
						}
					}

					if (index != -1) {
						parameters.Add(sql[i..index]);
					} else {
						parameters.Add(sql[i..]);
					}
				}
			}

			return parameters.ToArray();
		}

		private static object ConvertObject(object obj) {
			return obj switch {
				DateTime d => d.ToString("yyyy-MM-dd HH:mm:ss.fff"),
				decimal dec => dec.ToString(CultureInfo.InvariantCulture),
				_ => obj,
			};
		}

		private static readonly string[] connectionFailedLogs = new string[] {
			"Unable to connect to any of the specified MySQL hosts.",
			"Timeout expired.  The timeout period elapsed prior to obtaining a connection from the pool.  This may have occurred because all pooled connections were in use and max pool size was reached."
		};
		private static void LogException(Exception e, string query, object[] parameters) {
			if (Array.Exists(connectionFailedLogs, a => a == e.Message)) {
				if (DateTime.UtcNow >= _lastConnectionFailedLog.AddMinutes(5)) {
					_lastConnectionFailedLog = DateTime.UtcNow;
					Logger.Log(Logger.LogType.CriticalError, _className, "Connection to MySQL Server failed. Error Message: " + e.Message);
				}
			} else {
				Logger.Log(Logger.LogType.Error, _className, e.Message);
				string paramStr = parameters.Length > 0 ? " parameters " + string.Join(", ", parameters) : "";
				Logger.Log(Logger.LogType.Error, _className, query + paramStr);
			}
		}
	}
}
