using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataAccessLayer {
	public class Logger {
		public enum LogType {
			Debug,
			Warning,
			Error,
			CriticalError
		}

		public static void Log(LogType type, string callingClass, object log) {
			Console.WriteLine($"[{DateTime.Now:dd/MM/yyyy HH:mm:ss:fff}] [{type.ToString().ToUpper()}] {callingClass} - {log}");
		}
	}
}
