using System;
using System.IO;
using System.Net.Sockets;
using System.Net;
using System.Text.Json;
using ApiInterface.Models;
using System.Text.RegularExpressions;

namespace ApiInterface
{
    public class BinarySearchTree
    {
        private class Node
        {
            public string Key { get; set; }
            public Node Left { get; set; }
            public Node Right { get; set; }

            public Node(string key)
            {
                Key = key;
                Left = null;
                Right = null;
            }
        }

        private Node root;

        public BinarySearchTree()
        {
            root = null;
        }

        public void Insert(string key)
        {
            root = InsertRec(root, key);
        }

        private Node InsertRec(Node root, string key)
        {
            if (root == null)
            {
                root = new Node(key);
                return root;
            }

            if (string.Compare(key, root.Key) < 0)
            {
                root.Left = InsertRec(root.Left, key);
            }
            else if (string.Compare(key, root.Key) > 0)
            {
                root.Right = InsertRec(root.Right, key);
            }

            return root;
        }

        public bool Search(string key)
        {
            return SearchRec(root, key);
        }

        private bool SearchRec(Node root, string key)
        {
            if (root == null)
                return false;

            if (string.Compare(key, root.Key) == 0)
                return true;

            if (string.Compare(key, root.Key) < 0)
                return SearchRec(root.Left, key);

            return SearchRec(root.Right, key);
        }
        public bool Delete(string key)
        {
            root = DeleteRec(root, key);
            return root != null;
        }

        private Node DeleteRec(Node root, string key)
        {
            if (root == null)
                return root;

            if (string.Compare(key, root.Key) < 0)
            {
                root.Left = DeleteRec(root.Left, key);
            }
            else if (string.Compare(key, root.Key) > 0)
            {
                root.Right = DeleteRec(root.Right, key);
            }
            else
            {
                // Caso 1: Nodo sin hijos (hoja)
                if (root.Left == null && root.Right == null)
                {
                    return null;
                }
                // Caso 2: Nodo con un hijo
                else if (root.Left == null)
                {
                    return root.Right;
                }
                else if (root.Right == null)
                {
                    return root.Left;
                }
                // Caso 3: Nodo con dos hijos
                else
                {
                    root.Key = MinValue(root.Right);
                    root.Right = DeleteRec(root.Right, root.Key);
                }
            }

            return root;
        }

        private string MinValue(Node root)
        {
            string minv = root.Key;
            while (root.Left != null)
            {
                minv = root.Left.Key;
                root = root.Left;
            }
            return minv;
        }

    }

    public class BTree
    {
        private class Node
        {
            public List<string> Keys { get; private set; }
            public List<Node> Children { get; private set; }
            public bool IsLeaf { get; private set; }

            public Node(bool isLeaf)
            {
                Keys = new List<string>();
                Children = new List<Node>();
                IsLeaf = isLeaf;
            }
        }

        private Node root;
        private int degree;

        public BTree(int degree)
        {
            this.degree = degree;
            root = new Node(true);
        }

        public void Insert(string key)
        {
            if (root.Keys.Count == (2 * degree) - 1)
            {
                Node newRoot = new Node(false);
                newRoot.Children.Add(root);
                SplitChild(newRoot, 0, root);
                root = newRoot;
            }
            InsertNonFull(root, key);
        }

        private void SplitChild(Node parent, int index, Node fullChild)
        {
            Node newNode = new Node(fullChild.IsLeaf);
            parent.Children.Insert(index + 1, newNode);
            parent.Keys.Insert(index, fullChild.Keys[degree - 1]);

            for (int i = 0; i < degree - 1; i++)
            {
                newNode.Keys.Add(fullChild.Keys[i + degree]);
            }

            if (!fullChild.IsLeaf)
            {
                for (int i = 0; i < degree; i++)
                {
                    newNode.Children.Add(fullChild.Children[i + degree]);
                }
            }

            fullChild.Keys.RemoveRange(degree - 1, degree);
            if (!fullChild.IsLeaf)
            {
                fullChild.Children.RemoveRange(degree, degree);
            }
        }

        private void InsertNonFull(Node node, string key)
        {
            int i = node.Keys.Count - 1;
            if (node.IsLeaf)
            {
                node.Keys.Add(null);
                while (i >= 0 && string.Compare(key, node.Keys[i]) < 0)
                {
                    node.Keys[i + 1] = node.Keys[i];
                    i--;
                }
                node.Keys[i + 1] = key;
            }
            else
            {
                while (i >= 0 && string.Compare(key, node.Keys[i]) < 0)
                {
                    i--;
                }
                i++;
                if (node.Children[i].Keys.Count == (2 * degree) - 1)
                {
                    SplitChild(node, i, node.Children[i]);
                    if (string.Compare(key, node.Keys[i]) > 0)
                    {
                        i++;
                    }
                }
                InsertNonFull(node.Children[i], key);
            }
        }

        public bool Search(string key)
        {
            return SearchRec(root, key);
        }

        private bool SearchRec(Node node, string key)
        {
            int i = 0;
            while (i < node.Keys.Count && string.Compare(key, node.Keys[i]) > 0)
            {
                i++;
            }

            if (i < node.Keys.Count && string.Compare(key, node.Keys[i]) == 0)
            {
                return true;
            }

            if (node.IsLeaf)
            {
                return false;
            }

            return SearchRec(node.Children[i], key);
        }
        public void Delete(string key)
        {
            if (root == null)
            {
                Console.WriteLine("The tree is empty.");
                return;
            }

            DeleteRec(root, key);

            if (root.Keys.Count == 0 && !root.IsLeaf)
            {
                root = root.Children[0];
            }
        }

        private void DeleteRec(Node node, string key)
        {
            int idx = node.Keys.FindIndex(k => string.Compare(k, key) >= 0);

            if (idx < node.Keys.Count && node.Keys[idx] == key)
            {
                if (node.IsLeaf)
                {
                    node.Keys.RemoveAt(idx);
                }
                else
                {
                    DeleteFromNonLeaf(node, idx);
                }
            }
            else
            {
                if (node.IsLeaf)
                {
                    Console.WriteLine($"The key {key} does not exist in the tree.");
                    return;
                }

                bool flag = (idx == node.Keys.Count);

                if (node.Children[idx].Keys.Count < degree)
                {
                    Fill(idx, node);
                }

                if (flag && idx > node.Keys.Count)
                {
                    DeleteRec(node.Children[idx - 1], key);
                }
                else
                {
                    DeleteRec(node.Children[idx], key);
                }
            }
        }

        private void DeleteFromNonLeaf(Node node, int idx)
        {
            string key = node.Keys[idx];

            if (node.Children[idx].Keys.Count >= degree)
            {
                string pred = GetPredecessor(node, idx);
                node.Keys[idx] = pred;
                DeleteRec(node.Children[idx], pred);
            }
            else if (node.Children[idx + 1].Keys.Count >= degree)
            {
                string succ = GetSuccessor(node, idx);
                node.Keys[idx] = succ;
                DeleteRec(node.Children[idx + 1], succ);
            }
            else
            {
                Merge(node, idx);
                DeleteRec(node.Children[idx], key);
            }
        }
        private string GetPredecessor(Node node, int idx)
        {
            Node current = node.Children[idx];
            while (!current.IsLeaf)
            {
                current = current.Children[current.Keys.Count];
            }
            return current.Keys[current.Keys.Count - 1];
        }

        private string GetSuccessor(Node node, int idx)
        {
            Node current = node.Children[idx + 1];
            while (!current.IsLeaf)
            {
                current = current.Children[0];
            }
            return current.Keys[0];
        }

        private void Fill(int idx, Node node)
        {
            if (idx != 0 && node.Children[idx - 1].Keys.Count >= degree)
            {
                BorrowFromPrev(idx, node);
            }
            else if (idx != node.Keys.Count && node.Children[idx + 1].Keys.Count >= degree)
            {
                BorrowFromNext(idx, node);
            }
            else
            {
                if (idx != node.Keys.Count)
                {
                    Merge(node, idx);
                }
                else
                {
                    Merge(node, idx - 1);
                }
            }
        }

        private void BorrowFromPrev(int idx, Node node)
        {
            Node child = node.Children[idx];
            Node sibling = node.Children[idx - 1];

            for (int i = child.Keys.Count - 1; i >= 0; --i)
            {
                child.Keys[i + 1] = child.Keys[i];
            }

            if (!child.IsLeaf)
            {
                for (int i = child.Children.Count - 1; i >= 0; --i)
                {
                    child.Children[i + 1] = child.Children[i];
                }
            }

            child.Keys[0] = node.Keys[idx - 1];

            if (!node.IsLeaf)
            {
                child.Children[0] = sibling.Children[sibling.Keys.Count];
            }

            node.Keys[idx - 1] = sibling.Keys[sibling.Keys.Count - 1];
            sibling.Keys.RemoveAt(sibling.Keys.Count - 1);

            if (!sibling.IsLeaf)
            {
                sibling.Children.RemoveAt(sibling.Children.Count - 1);
            }
        }

        private void BorrowFromNext(int idx, Node node)
        {
            Node child = node.Children[idx];
            Node sibling = node.Children[idx + 1];

            child.Keys.Add(node.Keys[idx]);

            if (!child.IsLeaf)
            {
                child.Children.Add(sibling.Children[0]);
            }

            node.Keys[idx] = sibling.Keys[0];
            sibling.Keys.RemoveAt(0);

            if (!sibling.IsLeaf)
            {
                sibling.Children.RemoveAt(0);
            }
        }

        private void Merge(Node node, int idx)
        {
            Node child = node.Children[idx];
            Node sibling = node.Children[idx + 1];

            child.Keys.Add(node.Keys[idx]);

            for (int i = 0; i < sibling.Keys.Count; ++i)
            {
                child.Keys.Add(sibling.Keys[i]);
            }

            if (!child.IsLeaf)
            {
                for (int i = 0; i < sibling.Children.Count; ++i)
                {
                    child.Children.Add(sibling.Children[i]);
                }
            }

            node.Keys.RemoveAt(idx);
            node.Children.RemoveAt(idx + 1);
        }


    }

    public class Server
    {
        private static IPEndPoint serverEndPoint = new(IPAddress.Loopback, 11000);
        private static int supportedParallelConnections = 1;

        // Inicia el Servidor
        public static async Task Start()
        {
            LoadIndexesOnStartup();

            string currentDirectory = Directory.GetCurrentDirectory();
            Console.WriteLine($"Server is running. Current working directory: {currentDirectory}");

            using Socket listener = new(serverEndPoint.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
            listener.Bind(serverEndPoint);
            listener.Listen(supportedParallelConnections);
            Console.WriteLine($"Server ready at {serverEndPoint.ToString()}");

            while (true)
            {
                var handler = await listener.AcceptAsync();
                try
                {
                    var rawMessage = GetMessage(handler);
                    var response = ProcessRequest(rawMessage);
                    SendResponse(response, handler);
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex);
                    await SendErrorResponse("Unknown exception", handler);
                }
                finally
                {
                    handler.Close();
                }
            }
        }
        //===================================================================================================================================================
        private static string GetMessage(Socket handler)
        {
            using (NetworkStream stream = new NetworkStream(handler))
            using (StreamReader reader = new StreamReader(stream))
            {
                return reader.ReadLine() ?? string.Empty;
            }
        }

        // Detecta comandos
        private static string currentDatabase = string.Empty;
        private static string ProcessRequest(string request)
        {
            try
            {
                Console.WriteLine($"DEBUG: Received request: '{request}'");

                if (request.StartsWith("CREATE DATABASE"))
                {
                    var dbName = request.Split(' ')[2].TrimEnd(';');
                    return CreateDatabase(dbName);
                }
                else if (request.StartsWith("CREATE TABLE"))
                {
                    return CreateTable(request);
                }
                else if (request.StartsWith("SET DATABASE"))
                {
                    var dbName = request.Split(' ')[2].TrimEnd(';');
                    if (DatabaseExists(dbName))
                    {
                        currentDatabase = dbName;
                        return $"Database context set to '{dbName}'";
                    }
                    else
                    {
                        return $"Database '{dbName}' does not exist.";
                    }
                }
                if (request.Trim().Equals("SHOW TABLES", StringComparison.OrdinalIgnoreCase))
                {
                    ShowTables(); // Imprime directamente en la consola
                    return "Tables displayed.";
                }
                else if (request.StartsWith("SHOW TABLE", StringComparison.OrdinalIgnoreCase))
                {
                    var tableName = request.Split(' ')[2].TrimEnd(';');
                    ShowTableContents(tableName); // Imprime directamente en la consola
                    return "Table content displayed.";
                }
                else if (request.StartsWith("DROP TABLE"))
                {
                    var tableName = request.Split(' ')[2].TrimEnd(';');
                    return DropTable(tableName);
                }
                if (request.Trim().Equals("SHOW DATABASES", StringComparison.OrdinalIgnoreCase))
                {
                    ShowDatabases(); // Imprime directamente en la consola
                    return "Databases displayed.";
                }
                else if (request.Trim().Equals("SHOW DATABASE PATHS", StringComparison.OrdinalIgnoreCase))
                {
                    ShowDatabasePaths(); // Imprime directamente en la consola
                    return "Database paths displayed.";
                }
                else if (request.StartsWith("CREATE INDEX"))
                {
                    return CreateIndex(request);
                }
                else if (request.StartsWith("INSERT INTO"))
                {
                    return InsertIntoTable(request);
                }
                else if (request.StartsWith("UPDATE", StringComparison.OrdinalIgnoreCase))  // Manejar la sentencia UPDATE
                {
                    return UpdateTable(request);  // Llamar al método UpdateTable para procesar la consulta
                }
                else if (request.StartsWith("SELECT", StringComparison.OrdinalIgnoreCase))
                {
                    return SelectFromTable(request);
                }
                else if (request.StartsWith("DELETE", StringComparison.OrdinalIgnoreCase)) // Agregar manejo para DELETE
                {
                    return DeleteFromTable(request); // Llamar al método DeleteFromTable para procesar la consulta
                }
                else
                {
                    return "Unsupported command.";
                }
            }
            catch (Exception ex)
            {
                return $"Unknown exception: {ex.Message} - {ex.StackTrace}";
            }
        }


        //===================================================================================================================================================
        private static string CreateDatabase(string dbName)
        {
            // Crear una carpeta para ell DATABASE
            string path = Path.Combine(Directory.GetCurrentDirectory(), dbName);
            if (!Directory.Exists(path))
            {
                Directory.CreateDirectory(path);

                // Agregar información al (SystemDatabases.txt)
                string systemCatalogPath = Path.Combine(Directory.GetCurrentDirectory(), "SystemDatabases.txt");
                File.AppendAllText(systemCatalogPath, dbName + Environment.NewLine);

                return $"Database '{dbName}' created successfully.";
            }
            else
            {
                return $"Database '{dbName}' already exists.";
            }
        }

        private static void SendResponse(string response, Socket handler)
        {
            using (NetworkStream stream = new NetworkStream(handler))
            using (StreamWriter writer = new StreamWriter(stream))
            {
                writer.WriteLine(response);
            }
        }

        private static async Task SendErrorResponse(string reason, Socket handler)
        {
            using (NetworkStream stream = new NetworkStream(handler))
            using (StreamWriter writer = new StreamWriter(stream))
            {
                string errorMessage = $"Error: {reason}";
                await writer.WriteLineAsync(errorMessage);
            }
        }
        //===================================================================================================================================================
        private static void ShowTables()
        {
            if (string.IsNullOrEmpty(currentDatabase))
            {
                Console.WriteLine("No database selected. Please use 'SET DATABASE <db-name>' first.");
                return;
            }

            // Ruta al directorio de la base de datos actual
            string databasePath = Path.Combine(Directory.GetCurrentDirectory(), currentDatabase);

            if (!Directory.Exists(databasePath))
            {
                Console.WriteLine($"Error: Database '{currentDatabase}' does not exist.");
                return;
            }

            // Obtener todos los archivos con extensión .txt (suponiendo que representan tablas)
            string[] tableFiles = Directory.GetFiles(databasePath, "*.txt");

            // Verificar si hay tablas
            if (tableFiles.Length == 0)
            {
                Console.WriteLine("No tables found in the current database.");
                return;
            }

            // Extraer solo los nombres de las tablas sin la extensión
            string[] tableNames = Array.ConvertAll(tableFiles, tablePath => Path.GetFileNameWithoutExtension(tablePath));

            // Imprimir los nombres de las tablas
            Console.WriteLine("Tables:");
            foreach (string tableName in tableNames)
            {
                Console.WriteLine(tableName);
            }
        }

        private static void ShowTableContents(string tableName)
        {
            if (string.IsNullOrEmpty(currentDatabase))
            {
                Console.WriteLine("No database selected. Please use 'SET DATABASE <db-name>' first.");
                return;
            }

            // Ruta al archivo de la tabla en la base de datos actual
            string tablePath = Path.Combine(Directory.GetCurrentDirectory(), currentDatabase, $"{tableName}.txt");

            if (!File.Exists(tablePath))
            {
                Console.WriteLine($"Table '{tableName}' does not exist in the current database.");
                return;
            }

            // Leer todas las líneas del archivo de la tabla
            string[] lines = File.ReadAllLines(tablePath);

            // Verificar si la tabla tiene contenido
            if (lines.Length == 0)
            {
                Console.WriteLine($"Table '{tableName}' is empty.");
                return;
            }

            // Imprimir los contenidos de la tabla
            Console.WriteLine($"Table: {tableName}");
            foreach (string line in lines)
            {
                Console.WriteLine(line);
            }
        }

        private static void ShowDatabases()
        {
            string systemCatalogPath = Path.Combine(Directory.GetCurrentDirectory(), "SystemDatabases.txt");

            // Si el archivo no existe, mostrar un mensaje
            if (!File.Exists(systemCatalogPath))
            {
                Console.WriteLine("No databases found.");
                return;
            }

            // Leer las bases de datos
            var databases = File.ReadAllLines(systemCatalogPath);

            // Si el archivo está vacío o no tiene bases de datos, mostrar un mensaje
            if (databases.Length == 0 || string.IsNullOrWhiteSpace(string.Join("", databases)))
            {
                Console.WriteLine("No databases found.");
                return;
            }

            // Imprimir la lista de bases de datos
            Console.WriteLine("Databases:");
            foreach (var db in databases.Where(db => !string.IsNullOrWhiteSpace(db)))
            {
                Console.WriteLine(db);
            }
        }

        // Muestra los PATHs de las DATABASEs
        private static void ShowDatabasePaths()
        {
            string systemCatalogPath = Path.Combine(Directory.GetCurrentDirectory(), "SystemDatabases.txt");

            // Si el archivo no existe, mostrar un mensaje
            if (!File.Exists(systemCatalogPath))
            {
                Console.WriteLine("No databases found.");
                return;
            }

            // Leer las bases de datos
            var databases = File.ReadAllLines(systemCatalogPath);

            // Si el archivo está vacío o no tiene bases de datos, mostrar un mensaje
            if (databases.Length == 0 || string.IsNullOrWhiteSpace(string.Join("", databases)))
            {
                Console.WriteLine("No databases found.");
                return;
            }

            // Obtener las rutas completas de cada base de datos y mostrarlas
            Console.WriteLine("Database Paths:");
            foreach (var dbPath in databases.Where(db => !string.IsNullOrWhiteSpace(db))
                                            .Select(db => Path.Combine(Directory.GetCurrentDirectory(), db)))
            {
                Console.WriteLine(dbPath);
            }
        }
        //===================================================================================================================================================
        private static string CreateTable(string request)
        {
            if (string.IsNullOrEmpty(currentDatabase))
            {
                return "No database selected. Please use 'SET DATABASE <db-name>' first.";
            }

            // Parsear la sentencia CREATE TABLE
            string[] parts = request.Split(' ');
            string tableName = parts[2];
            string columnsDefinition = request.Substring(request.IndexOf("(") + 1);
            columnsDefinition = columnsDefinition.TrimEnd(')', ';');

            // Validar y procesar la definición de las columnas
            string[] columns = columnsDefinition.Split(',');
            foreach (string column in columns)
            {
                string[] columnParts = column.Trim().Split(' ');
                string columnName = columnParts[0];
                string columnType = columnParts[1];

                // Validar tipos de datos
                if (!IsValidDataType(columnType))
                {
                    return $"Error: Invalid data type '{columnType}' for column '{columnName}'.";
                }

                // Validar restricciones
                string constraint = columnParts.Length > 2 ? columnParts[2].ToUpper() : "NULL"; // Si no se especifica, se asume NULL
                if (constraint != "NULL" && constraint != "NOT NULL")
                {
                    return $"Error: Invalid constraint '{constraint}' for column '{columnName}'.";
                }
            }

            // Crear el archivo de la tabla
            string tablePath = Path.Combine(Directory.GetCurrentDirectory(), currentDatabase, $"{tableName}.txt");

            if (!File.Exists(tablePath))
            {
                // Guardar la definición de la tabla en una sola línea en el archivo
                using (StreamWriter writer = new StreamWriter(tablePath))
                {
                    writer.Write(string.Join(", ", columns.Select(c => c.Trim())));
                }

                // Actualizar el archivo SystemTables.txt
                string systemTablesPath = Path.Combine(Directory.GetCurrentDirectory(), "SystemTables.txt");
                File.AppendAllText(systemTablesPath, $"{tableName} ({columnsDefinition}){Environment.NewLine}");

                return $"Table '{tableName}' created successfully with columns: {columnsDefinition}";
            }
            else
            {
                return $"Table '{tableName}' already exists.";
            }
        }
        // Método para validar los tipos de datos
        private static bool IsValidDataType(string columnType)
        {
            string[] validTypes = { "INTEGER", "INT", "DOUBLE", "VARCHAR", "DATETIME" };
            if (columnType.StartsWith("VARCHAR")) // Verificar si es VARCHAR con longitud
            {
                return columnType.Contains("(") && columnType.Contains(")");
            }
            return validTypes.Contains(columnType.ToUpper());
        }

        private static bool DatabaseExists(string dbName)
        {
            string systemCatalogPath = Path.Combine(Directory.GetCurrentDirectory(), "SystemDatabases.txt");
            return File.ReadLines(systemCatalogPath).Any(line => line.Trim() == dbName);
        }

        private static bool TableExists(string tableName)
        {
            string systemTablesPath = Path.Combine(Directory.GetCurrentDirectory(), "SystemTables.txt");
            return File.ReadLines(systemTablesPath).Any(line => line.StartsWith(tableName + " "));
        }
        //===================================================================================================================================================
        private static string DropTable(string tableName)
        {
            if (string.IsNullOrEmpty(currentDatabase))
            {
                return "No database selected. Please use 'SET DATABASE <db-name>' first.";
            }

            // Verificar si la tabla existe
            string tablePath = Path.Combine(Directory.GetCurrentDirectory(), currentDatabase, $"{tableName}.txt");

            if (!File.Exists(tablePath))
            {
                return $"Table '{tableName}' does not exist.";
            }

            // Verificar si la tabla está vacía (consideramos que una tabla está vacía si no contiene datos)
            var lines = File.ReadAllLines(tablePath);
            if (lines.Length > 1) // Si hay más de una línea, verificamos si tiene datos
            {
                bool hasData = lines.Skip(1).Any(line => !string.IsNullOrWhiteSpace(line)); // Verificar si hay datos después de los encabezados
                if (hasData)
                {
                    return $"Table '{tableName}' is not empty. It cannot be dropped.";
                }
            }

            // Eliminar la tabla
            File.Delete(tablePath);

            // Eliminar los índices asociados a esta tabla
            RemoveIndexesForTable(tableName);

            // Actualizar el archivo SystemTables.txt para eliminar la referencia a la tabla
            string systemTablesPath = Path.Combine(Directory.GetCurrentDirectory(), "SystemTables.txt");
            var updatedTables = File.ReadAllLines(systemTablesPath).Where(line => !line.StartsWith(tableName)).ToArray();
            File.WriteAllLines(systemTablesPath, updatedTables);

            return $"Table '{tableName}' dropped successfully.";
        }

        // Método para eliminar los índices asociados a una tabla
        private static void RemoveIndexesForTable(string tableName)
        {
            string systemIndexesPath = Path.Combine(Directory.GetCurrentDirectory(), "SystemIndexes.txt");

            if (File.Exists(systemIndexesPath))
            {
                var indexLines = File.ReadAllLines(systemIndexesPath).ToList();
                var updatedIndexes = indexLines.Where(indexLine =>
                {
                    var parts = indexLine.Split(' ');
                    if (parts.Length != 6)
                    {
                        Console.WriteLine($"Warning: Malformed index entry in SystemIndexes.txt: {indexLine}. Skipping.");
                        return true;
                    }

                    string indexedTableNameWithColumn = parts[2];
                    string indexedTableName = indexedTableNameWithColumn.Split('(')[0];

                    // Eliminar índices asociados a la tabla que se va a eliminar
                    if (indexedTableName == tableName)
                    {
                        Console.WriteLine($"Removing index '{parts[0]}' associated with table '{tableName}'");
                        return false; // Eliminar este índice
                    }

                    return true;
                }).ToArray();

                // Sobrescribir SystemIndexes.txt con los índices restantes
                File.WriteAllLines(systemIndexesPath, updatedIndexes);
            }
        }

        //===================================================================================================================================================
        private static Dictionary<string, object> indexes = new Dictionary<string, object>();

        private static string CreateIndex(string request)
        {
            // Verificación de base de datos seleccionada
            if (string.IsNullOrEmpty(currentDatabase))
            {
                return "No database selected. Please use 'SET DATABASE <db-name>' first.";
            }

            // Regex para identificar la creación de índices
            var match = Regex.Match(request, @"CREATE\s+INDEX\s+(\w+)\s+ON\s+(\w+)\((\w+)\)\s+OF\s+TYPE\s+(\w+);?", RegexOptions.IgnoreCase);

            if (!match.Success)
            {
                return "Error: Invalid CREATE INDEX command. Expected format: CREATE INDEX <index_name> ON <table_name>(<column_name>) OF TYPE <index_type>";
            }

            // Extraer las partes del comando
            string indexName = match.Groups[1].Value;
            string tableName = match.Groups[2].Value;
            string columnName = match.Groups[3].Value;
            string indexType = match.Groups[4].Value;

            // Verificar si la tabla existe
            string tablePath = Path.Combine(Directory.GetCurrentDirectory(), currentDatabase, $"{tableName}.txt");
            if (!File.Exists(tablePath))
            {
                return $"Table '{tableName}' does not exist.";
            }

            // Leer las líneas de la tabla
            var lines = File.ReadAllLines(tablePath);
            string[] headers = lines[0].Split(',').Select(h => h.Trim().Split(' ')[0]).ToArray();
            int columnIndex = Array.IndexOf(headers, columnName);
            if (columnIndex == -1)
            {
                return $"Error: Column '{columnName}' not found in table '{tableName}'";
            }

            // Crear el índice
            if (indexType.ToUpper() == "BTREE")
            {
                indexes[columnName] = new BTree(3); // Ajustar el grado si es necesario
            }
            else if (indexType.ToUpper() == "BST")
            {
                indexes[columnName] = new BinarySearchTree();
            }
            else
            {
                return "Invalid index type. Use 'BTREE' or 'BST'.";
            }

            // Inserción de valores en el índice creado
            for (int i = 1; i < lines.Length; i++)
            {
                string[] row = lines[i].Split(',').Select(val => val.Trim()).ToArray();
                string valueToIndex = row[columnIndex];

                if (indexes[columnName] is BTree btree)
                {
                    btree.Insert(valueToIndex);
                }
                else if (indexes[columnName] is BinarySearchTree bst)
                {
                    bst.Insert(valueToIndex);
                }
            }

            // Guardar la creación del índice en SystemIndexes.txt
            try
            {
                string systemIndexesPath = Path.Combine(Directory.GetCurrentDirectory(), "SystemIndexes.txt");

                // Asegurarse de que el archivo existe o se crea correctamente
                if (!File.Exists(systemIndexesPath))
                {
                    File.Create(systemIndexesPath).Dispose();  // Crea el archivo si no existe
                }

                // Añadir la información del índice al archivo
                string indexEntry = $"{indexName} ON {tableName}({columnName}) OF TYPE {indexType}{Environment.NewLine}";
                File.AppendAllText(systemIndexesPath, indexEntry);

                return $"Index '{indexName}' created successfully on column '{columnName}' of table '{tableName}' with type '{indexType}'.";
            }
            catch (Exception ex)
            {
                return $"Error: Could not write index to SystemIndexes.txt - {ex.Message}";
            }
        }


        private static void LoadIndexesOnStartup()
        {
            // Ruta del archivo de índices
            string systemIndexesPath = Path.Combine(Directory.GetCurrentDirectory(), "SystemIndexes.txt");

            // Verificar si existe el archivo de índices
            if (File.Exists(systemIndexesPath))
            {
                var indexLines = File.ReadAllLines(systemIndexesPath);

                foreach (var indexLine in indexLines)
                {
                    if (string.IsNullOrWhiteSpace(indexLine))
                    {
                        continue; // Saltar líneas vacías
                    }

                    var parts = indexLine.Split(new[] { ' ' }, StringSplitOptions.RemoveEmptyEntries);

                    if (parts.Length != 6) // Asegurarse de que la entrada tiene el formato adecuado
                    {
                        Console.WriteLine($"Error: Malformed index entry: '{indexLine}'");
                        continue;
                    }

                    string indexName = parts[0];
                    string tableNameWithColumn = parts[2];
                    string tableName = tableNameWithColumn.Split('(')[0];  // Extraer el nombre de la tabla
                    string columnName = tableNameWithColumn.Split('(')[1].TrimEnd(')');  // Extraer el nombre de la columna
                    string indexType = parts[5]; // Tipo de índice

                    // Buscar en todas las bases de datos para ver si la tabla existe
                    string[] databases = Directory.GetDirectories(Directory.GetCurrentDirectory());
                    bool tableFound = false;

                    foreach (var dbPath in databases)
                    {
                        string dbName = Path.GetFileName(dbPath);
                        string tablePath = Path.Combine(dbPath, $"{tableName}.txt");

                        if (File.Exists(tablePath))
                        {
                            Console.WriteLine($"Found table '{tableName}' in database '{dbName}'. Rebuilding index '{indexName}'.");

                            // Leer las líneas de la tabla
                            var lines = File.ReadAllLines(tablePath);

                            // Verificar si la columna existe
                            string[] headers = lines[0].Split(',').Select(h => h.Trim().Split(' ')[0]).ToArray();
                            int columnIndex = Array.IndexOf(headers, columnName);
                            if (columnIndex == -1)
                            {
                                Console.WriteLine($"Error: Column '{columnName}' not found in table '{tableName}' while loading index '{indexName}'.");
                                continue;
                            }

                            // Recrear el índice en memoria
                            if (indexType.ToUpper() == "BTREE")
                            {
                                indexes[columnName] = new BTree(3); // Ajustar el grado si es necesario
                                Console.WriteLine($"Rebuilding BTREE index '{indexName}' for table '{tableName}', column '{columnName}'.");
                            }
                            else if (indexType.ToUpper() == "BST")
                            {
                                indexes[columnName] = new BinarySearchTree();
                                Console.WriteLine($"Rebuilding BST index '{indexName}' for table '{tableName}', column '{columnName}'.");
                            }
                            else
                            {
                                Console.WriteLine($"Error: Invalid index type '{indexType}' for index '{indexName}'");
                                continue;
                            }

                            // Insertar todos los valores de la columna en el índice
                            for (int i = 1; i < lines.Length; i++)
                            {
                                string[] row = lines[i].Split(',').Select(val => val.Trim()).ToArray();
                                string valueToIndex = row[columnIndex];

                                if (indexes[columnName] is BTree btree)
                                {
                                    btree.Insert(valueToIndex);
                                }
                                else if (indexes[columnName] is BinarySearchTree bst)
                                {
                                    bst.Insert(valueToIndex);
                                }
                            }

                            tableFound = true;
                            break; // Dejar de buscar en otras bases de datos si la tabla se encuentra
                        }
                    }

                    if (!tableFound)
                    {
                        Console.WriteLine($"Warning: Table '{tableName}' not found in any database while loading index '{indexName}'. Skipping.");
                    }
                }
            }
            else
            {
                Console.WriteLine("No indexes found to load.");
            }
        }


        //===================================================================================================================================================
        private static string InsertIntoTable(string request)
        {
            if (string.IsNullOrEmpty(currentDatabase))
            {
                return "No database selected. Please use 'SET DATABASE <db-name>' first.";
            }

            // Parsear la sentencia INSERT INTO
            string[] parts = request.Split(new[] { "INTO", "VALUES" }, StringSplitOptions.None);
            string tableName = parts[1].Trim();
            string valuesPart = parts[2].Trim().Trim('(', ')');

            // Verificar si la tabla existe
            string tablePath = Path.Combine(Directory.GetCurrentDirectory(), currentDatabase, $"{tableName}.txt");
            if (!File.Exists(tablePath))
            {
                return $"Table '{tableName}' does not exist.";
            }

            // Leer la definición de las columnas de la tabla
            var lines = File.ReadAllLines(tablePath);
            if (lines.Length == 0)
            {
                return $"Error: Table '{tableName}' has no columns defined.";
            }

            string[] columns = lines[0].Split(',');

            // Validar que la cantidad de valores coincida con la cantidad de columnas
            string[] values = valuesPart.Split(',');
            if (values.Length != columns.Length)
            {
                return "Error: The number of values does not match the number of columns.";
            }

            // Validar los tipos de datos para cada valor
            for (int i = 0; i < values.Length; i++)
            {
                string column = columns[i].Trim();
                string value = values[i].Trim().Trim('\'');

                // Validar y convertir el tipo DateTime si es necesario
                if (column.Contains("DATETIME", StringComparison.OrdinalIgnoreCase))
                {
                    if (!DateTime.TryParse(value, out _))
                    {
                        return $"Error: Invalid DateTime format for column '{column}'.";
                    }
                }

                if (!IsValidDataTypeForColumn(value, column))
                {
                    return $"Error: Invalid data type for column '{column}'.";
                }
            }

            // Verificar si alguno de los valores ya existe en un índice (duplicado)
            if (DuplicateInIndex(tableName, columns, values))
            {
                return "Error: Duplicate value found in an indexed column. Insertion aborted.";
            }

            // Insertar los valores en la tabla, asegurándonos de que se inserten después de los encabezados
            using (StreamWriter writer = new StreamWriter(tablePath, append: true))
            {
                writer.WriteLine(string.Join(",", values.Select(v => v.Trim())));
            }

            // Actualizar los índices asociados después de insertar los valores
            UpdateIndexesAfterInsert(tableName, columns, values);

            return $"Values inserted into '{tableName}' successfully.";
        }



        // Método para verificar si el valor ya existe en alguna columna con índice
        private static bool DuplicateInIndex(string tableName, string[] columns, string[] values)
        {
            string systemIndexesPath = Path.Combine(Directory.GetCurrentDirectory(), "SystemIndexes.txt");

            if (File.Exists(systemIndexesPath))
            {
                var indexLines = File.ReadAllLines(systemIndexesPath);
                foreach (var indexLine in indexLines)
                {
                    var parts = indexLine.Split(' ');

                    if (parts.Length != 6) // Asegurarse de que se están leyendo 6 partes
                    {
                        Console.WriteLine($"Warning: Malformed index entry in SystemIndexes.txt: {indexLine}. Skipping.");
                        continue;
                    }

                    string indexedTableNameWithColumn = parts[2];
                    string indexedTableName = indexedTableNameWithColumn.Split('(')[0];  // Extraer el nombre de la tabla
                    string indexedColumnName = indexedTableNameWithColumn.Split('(')[1].TrimEnd(')');  // Extraer el nombre de la columna
                    string indexType = parts[5]; // Tipo de índice

                    if (indexedTableName == tableName)
                    {
                        // Extraer los nombres de las columnas y verificar el índice
                        string[] columnsWithTypes = columns;
                        string[] columnNames = columnsWithTypes.Select(c => c.Trim().Split(' ')[0]).ToArray();  // Extraer solo los nombres de las columnas

                        int columnIndex = Array.IndexOf(columnNames, indexedColumnName);  // Comparar solo los nombres

                        if (columnIndex == -1)
                        {
                            Console.WriteLine($"Warning: Column '{indexedColumnName}' not found in table '{tableName}'. Skipping index check.");
                            continue;
                        }

                        string valueToCheck = values[columnIndex];

                        // Verificar si el valor ya existe en el índice (BTREE o BST)
                        if (indexes.TryGetValue(indexedColumnName, out var indexObject))
                        {
                            if (indexType.ToUpper() == "BTREE" && indexObject is BTree btree)
                            {
                                if (btree.Search(valueToCheck)) // Verificar si el valor ya existe en el BTREE
                                {
                                    return true; // Duplicado encontrado en BTREE
                                }
                            }
                            else if (indexType.ToUpper() == "BST" && indexObject is BinarySearchTree bst)
                            {
                                if (bst.Search(valueToCheck)) // Verificar si el valor ya existe en el BST
                                {
                                    return true; // Duplicado encontrado en BST
                                }
                            }
                        }
                        else
                        {
                            Console.WriteLine($"Warning: Index object for column '{indexedColumnName}' not found. Skipping.");
                        }
                    }
                }
            }

            return false; // No se encontraron duplicados
        }





        private static bool IsValidDataTypeForColumn(string value, string column)
        {
            // Aquí implementarías la lógica para verificar los tipos de datos de cada columna.
            // Esto podría depender de si la columna es de tipo INT, VARCHAR, DATETIME, etc.
            // Por ejemplo:
            if (column.ToUpper().Contains("INT"))
            {
                return int.TryParse(value, out _);
            }
            else if (column.ToUpper().Contains("VARCHAR"))
            {
                return value.Length <= 50;  // Ajusta el tamaño de VARCHAR según la definición
            }
            else if (column.ToUpper().Contains("DATETIME"))
            {
                return DateTime.TryParse(value, out _);
            }
            return true; // Para otros tipos de datos
        }

        private static void UpdateIndexesAfterInsert(string tableName, string[] columns, string[] values)
        {
            string systemIndexesPath = Path.Combine(Directory.GetCurrentDirectory(), "SystemIndexes.txt");

            if (File.Exists(systemIndexesPath))
            {
                var indexLines = File.ReadAllLines(systemIndexesPath);
                foreach (var indexLine in indexLines)
                {
                    var parts = indexLine.Split(' ');

                    if (parts.Length != 6)
                    {
                        Console.WriteLine($"Warning: Malformed index entry in SystemIndexes.txt: {indexLine}. Skipping.");
                        continue;
                    }

                    string indexedTableNameWithColumn = parts[2];
                    string indexedTableName = indexedTableNameWithColumn.Split('(')[0];  // Extraer el nombre de la tabla
                    string indexedColumnName = indexedTableNameWithColumn.Split('(')[1].TrimEnd(')');  // Extraer el nombre de la columna
                    string indexType = parts[5]; // Tipo de índice

                    if (indexedTableName == tableName)
                    {
                        // Extraer solo los nombres de las columnas de la tabla, sin los tipos
                        string[] columnNames = columns.Select(c => c.Trim().Split(' ')[0]).ToArray();  // Extraer solo los nombres de las columnas

                        int columnIndex = Array.IndexOf(columnNames, indexedColumnName);  // Comparar solo los nombres

                        if (columnIndex == -1)
                        {
                            Console.WriteLine($"Warning: Column '{indexedColumnName}' not found in table '{tableName}'. Skipping index update.");
                            continue;
                        }

                        string newValue = values[columnIndex];

                        // Insertar el nuevo valor en los índices correctos
                        if (indexType.ToUpper() == "BTREE" && indexes.TryGetValue(indexedColumnName, out var btreeObj) && btreeObj is BTree btree)
                        {
                            Console.WriteLine($"Updating BTREE index for column '{indexedColumnName}' in table '{tableName}' with new value '{newValue}'.");
                            btree.Insert(newValue);  // Insertar el valor en el índice BTREE
                        }
                        else if (indexType.ToUpper() == "BST" && indexes.TryGetValue(indexedColumnName, out var bstObj) && bstObj is BinarySearchTree bst)
                        {
                            Console.WriteLine($"Updating BST index for column '{indexedColumnName}' in table '{tableName}' with new value '{newValue}'.");
                            bst.Insert(newValue);  // Insertar el valor en el índice BST
                        }
                    }
                }
            }
        }



        //===================================================================================================================================================
        private static string SelectFromTable(string request)
        {
            Console.WriteLine($"DEBUG: Received SELECT command: '{request}'");

            var match = Regex.Match(request, @"^\s*SELECT\s+(\*|[\w\s,]+)\s+FROM\s+(\w+)(?:\s+WHERE\s+(.+?))?(?:\s+ORDER\s+BY\s+(\w+)(?:\s+(ASC|DESC))?)?\s*;?\s*$", RegexOptions.IgnoreCase);

            if (!match.Success)
            {
                return "Error: Invalid SELECT command.";
            }

            // Extraer columnas, tabla, cláusula WHERE y orden
            string columns = match.Groups[1].Value.Trim();
            string tableName = match.Groups[2].Value.Trim();
            string whereClause = match.Groups[3].Value?.Trim();
            string orderByColumn = match.Groups[4].Value?.Trim();
            string orderDirection = match.Groups[5].Value?.Trim()?.ToUpper();

            // Verificar que la tabla exista
            string tablePath = Path.Combine(Directory.GetCurrentDirectory(), currentDatabase, $"{tableName}.txt");
            if (!File.Exists(tablePath))
            {
                return $"Table '{tableName}' does not exist.";
            }

            // Leer la tabla
            var lines = File.ReadAllLines(tablePath);
            if (lines.Length <= 1)
            {
                return $"Error: Table '{tableName}' has no data.";
            }

            // Obtener los encabezados de la tabla y quitar cualquier parte relacionada con el tipo de datos
            string[] headers = lines[0].Split(',')
                .Select(h => h.Trim().Split(new[] { ' ' }, StringSplitOptions.RemoveEmptyEntries)[0])
                .ToArray();

            string[] rows = lines.Skip(1).ToArray();  // Obtener las filas como un array de strings

            // Aplicar la cláusula WHERE, si existe
            List<string[]> result = string.IsNullOrEmpty(whereClause)
                ? rows.Select(row => row.Split(',').Select(val => val.Trim().Trim('\'')).ToArray()).ToList() // Si no hay WHERE, devolver todas las filas
                : ApplyWhereWithIndex(headers, rows, whereClause, tableName);  // Aplicar WHERE con o sin índice

            // Asegurarse de que el filtro WHERE funcione correctamente
            if (result.Count == 0)
            {
                return "No rows match the WHERE condition.";
            }

            // Aplicar ORDER BY si existe
            if (!string.IsNullOrEmpty(orderByColumn))
            {
                int orderByIndex = Array.IndexOf(headers, orderByColumn);
                if (orderByIndex == -1)
                {
                    return $"Error: Column '{orderByColumn}' not found in table '{tableName}'.";
                }

                // Ordenar usando QuickSort
                QuickSort(result, orderByIndex, 0, result.Count - 1, orderDirection == "DESC");
            }

            // Mostrar los resultados filtrados y ordenados
            string output = string.Join(",", headers) + Environment.NewLine; // Encabezados
            output += string.Join(Environment.NewLine, result.Select(row => string.Join(",", row))); // Filas
            return output;
        }

        private static List<string[]> ApplyWhereWithIndex(string[] headers, string[] rows, string whereClause, string tableName)
        {
            var match = Regex.Match(whereClause, @"(\w+)\s*(=|>|<|like|not)\s*(.+)", RegexOptions.IgnoreCase);
            if (!match.Success)
            {
                throw new Exception("Error: Invalid WHERE clause.");
            }

            string columnName = match.Groups[1].Value.Trim();
            string operatorType = match.Groups[2].Value.Trim();
            string value = match.Groups[3].Value.Trim().Trim('\'');

            int columnIndex = Array.IndexOf(headers, columnName);
            if (columnIndex == -1)
            {
                throw new Exception($"Error: Column '{columnName}' not found in table.");
            }

            // Buscar si hay un índice disponible para esta columna
            if (indexes.ContainsKey(columnName))
            {
                Console.WriteLine($"Using index for column '{columnName}' in WHERE clause.");
                if (indexes[columnName] is BinarySearchTree bst)
                {
                    return rows.Select(row => row.Split(',').Select(val => val.Trim().Trim('\'')).ToArray())
                               .Where(row => bst.Search(row[columnIndex]))  // Verificar si el valor está en el índice
                               .ToList(); // Devolver la fila completa como string[]
                }
                else if (indexes[columnName] is BTree btree)
                {
                    return rows.Select(row => row.Split(',').Select(val => val.Trim().Trim('\'')).ToArray())
                               .Where(row => btree.Search(row[columnIndex]))  // Verificar si el valor está en el índice
                               .ToList(); // Devolver la fila completa como string[]
                }
            }

            // Si no hay índice, usar búsqueda secuencial
            return rows.Select(row => row.Split(',')
                       .Select(val => val.Trim().Trim('\'')) // Remover comillas
                       .ToArray())
                       .Where(row => ApplyWhereOperator(row[columnIndex], operatorType, value, columnName == "ID" || columnName == "Age")) // Detectar si la columna es numérica
                       .ToList();
        }


        // QuickSort para ordenar las filas
        private static void QuickSort(List<string[]> result, int columnIndex, int left, int right, bool desc)
        {
            if (left < right)
            {
                int pivotIndex = Partition(result, columnIndex, left, right, desc);
                QuickSort(result, columnIndex, left, pivotIndex - 1, desc);
                QuickSort(result, columnIndex, pivotIndex + 1, right, desc);
            }
        }

        // Método de partición para QuickSort
        private static int Partition(List<string[]> result, int columnIndex, int left, int right, bool desc)
        {
            string[] pivot = result[right];
            int i = left - 1;

            for (int j = left; j < right; j++)
            {
                bool comparison = desc
                    ? string.Compare(result[j][columnIndex], pivot[columnIndex]) > 0  // Orden descendente
                    : string.Compare(result[j][columnIndex], pivot[columnIndex]) < 0; // Orden ascendente

                if (comparison)
                {
                    i++;
                    (result[i], result[j]) = (result[j], result[i]);  // Intercambiar filas
                }
            }

            (result[i + 1], result[right]) = (result[right], result[i + 1]);  // Mover el pivote a su lugar
            return i + 1;
        }


        //===================================================================================================================================================
        private static string UpdateTable(string request)
        {
            if (string.IsNullOrEmpty(currentDatabase))
            {
                return "No database selected. Please use 'SET DATABASE <db-name>' first.";
            }

            // Expresión regular mejorada para detectar el comando UPDATE
            var match = Regex.Match(request, @"UPDATE\s+(\w+)\s+SET\s+(\w+)\s*=\s*'?([^']+)'?\s*(?:WHERE\s+(.+))?;", RegexOptions.IgnoreCase);

            if (!match.Success)
            {
                return "Error: Invalid UPDATE command.";
            }

            string tableName = match.Groups[1].Value.Trim();   // Nombre de la tabla
            string columnName = match.Groups[2].Value.Trim();  // Nombre de la columna a actualizar
            string newValue = match.Groups[3].Value.Trim().Trim('\'');    // Nuevo valor que se va a establecer (puede ser numérico o de texto)
            string whereClause = match.Groups[4].Value?.Trim(); // Condición WHERE (opcional)

            // Verificar si la tabla existe
            string tablePath = Path.Combine(Directory.GetCurrentDirectory(), currentDatabase, $"{tableName}.txt");
            if (!File.Exists(tablePath))
            {
                return $"Table '{tableName}' does not exist.";
            }

            // Leer el contenido de la tabla
            var lines = File.ReadAllLines(tablePath).ToList();
            if (lines.Count <= 1)
            {
                return $"Error: Table '{tableName}' has no data.";
            }

            // Obtener los encabezados de la tabla, conservando nombre y tipo de las columnas
            string[] fullHeaders = lines[0].Split(',').Select(h => h.Trim()).ToArray();
            string[] headers = fullHeaders.Select(h => h.Split(' ')[0]).ToArray(); // Solo nombres de columnas

            // Validar que la columna a actualizar exista
            int columnIndex = Array.IndexOf(headers, columnName);
            if (columnIndex == -1)
            {
                return $"Error: Column '{columnName}' not found in table '{tableName}'.";
            }

            // Filas de la tabla (excluyendo los encabezados)
            var rows = lines.Skip(1).Select(row => row.Split(',').Select(val => val.Trim().Trim('\'')).ToArray()).ToList();

            // Aplicar la cláusula WHERE si está presente, de lo contrario actualizar todas las filas
            List<string[]> rowsToUpdate = string.IsNullOrEmpty(whereClause)
                ? rows // Si no hay WHERE, actualizar todas las filas
                : ApplyWhereWithIndex(headers, rows.Select(row => string.Join(",", row)).ToArray(), whereClause, tableName); // Aplicar WHERE

            // Asegurarse de que hay filas que actualizar
            if (rowsToUpdate.Count == 0)
            {
                return "No rows match the WHERE condition.";
            }

            // Actualizar las filas afectadas
            foreach (var row in rowsToUpdate)
            {
                row[columnIndex] = newValue;
            }

            // Escribir los cambios en el archivo, reemplazando el contenido anterior
            using (var writer = new StreamWriter(tablePath))
            {
                // Escribir los encabezados nuevamente (con nombre y tipo)
                writer.WriteLine(string.Join(",", fullHeaders));

                // Escribir las filas actualizadas
                foreach (var row in rows)
                {
                    writer.WriteLine(string.Join(",", row));
                }
            }

            // Actualizar los índices después de realizar el cambio
            UpdateIndexesAfterUpdate(tableName, columnName, newValue, rowsToUpdate);

            return $"{rowsToUpdate.Count} row(s) updated in table '{tableName}'.";
        }




        // Método para actualizar los índices después de una actualización
        private static void UpdateIndexesAfterUpdate(string tableName, string columnName, string newValue, List<string[]> rowsToUpdate)
        {
            string systemIndexesPath = Path.Combine(Directory.GetCurrentDirectory(), "SystemIndexes.txt");

            if (File.Exists(systemIndexesPath))
            {
                var indexLines = File.ReadAllLines(systemIndexesPath);
                foreach (var indexLine in indexLines)
                {
                    var parts = indexLine.Split(' ');

                    if (parts.Length != 6)
                    {
                        Console.WriteLine($"Warning: Malformed index entry in SystemIndexes.txt: {indexLine}. Skipping.");
                        continue;
                    }

                    string indexedTableNameWithColumn = parts[2];
                    string indexedColumnName = indexedTableNameWithColumn.Split('(')[1].TrimEnd(')');

                    if (indexedColumnName == columnName)
                    {
                        Console.WriteLine($"Updating index for column '{columnName}' in table '{tableName}' after UPDATE.");

                        // Eliminar las entradas anteriores del índice y agregar las nuevas
                        if (indexes[columnName] is BTree btree)
                        {
                            foreach (var row in rowsToUpdate)
                            {
                                btree.Insert(newValue);
                            }
                        }
                        else if (indexes[columnName] is BinarySearchTree bst)
                        {
                            foreach (var row in rowsToUpdate)
                            {
                                bst.Insert(newValue);
                            }
                        }
                    }
                }
            }
        }

        //===================================================================================================================================================
        private static string DeleteFromTable(string request)
        {
            if (string.IsNullOrEmpty(currentDatabase))
            {
                return "No database selected. Please use 'SET DATABASE <db-name>' first.";
            }

            // Expresión regular ajustada para detectar comparaciones en WHERE (>, <, >=, <=, =, etc.)
            var match = Regex.Match(request, @"DELETE\s+FROM\s+(\w+)\s+WHERE\s+(\w+)\s*(=|>|<|>=|<=)\s*('?[\w\d\s-]+'?);?", RegexOptions.IgnoreCase);
            if (!match.Success)
            {
                return "Error: Invalid DELETE command.";
            }

            string tableName = match.Groups[1].Value.Trim(); // Nombre de la tabla
            string columnName = match.Groups[2].Value.Trim(); // Nombre de la columna en la condición WHERE
            string operatorType = match.Groups[3].Value.Trim(); // Operador de comparación (=, >, <, >=, <=)
            string value = match.Groups[4].Value.Trim().Trim('\''); // Valor en la condición WHERE (se remueven las comillas simples si existen)

            // Verificar si la tabla existe
            string tablePath = Path.Combine(Directory.GetCurrentDirectory(), currentDatabase, $"{tableName}.txt");
            if (!File.Exists(tablePath))
            {
                return $"Table '{tableName}' does not exist.";
            }

            // Leer el contenido de la tabla
            var lines = File.ReadAllLines(tablePath).ToList();
            if (lines.Count <= 1)
            {
                return $"Error: Table '{tableName}' has no data.";
            }

            // Obtener los encabezados de la tabla
            string[] headers = lines[0].Split(',').Select(h => h.Trim().Split(' ')[0]).ToArray();
            int columnIndex = Array.IndexOf(headers, columnName);

            if (columnIndex == -1)
            {
                return $"Error: Column '{columnName}' not found in table '{tableName}'.";
            }

            // Determinar si la columna es numérica o no
            bool isNumeric = columnName.Equals("ID", StringComparison.OrdinalIgnoreCase) || columnName.Equals("Age", StringComparison.OrdinalIgnoreCase);

            // Filtrar las filas que cumplan con la cláusula WHERE
            List<string[]> rowsToDelete = lines
                .Skip(1) // Saltar encabezado
                .Select(row => row.Split(',').Select(val => val.Trim().Trim('\'')).ToArray())
                .Where(row => ApplyWhereOperator(row[columnIndex], operatorType, value, isNumeric)) // Aplicar WHERE
                .ToList();

            // Verificar si hay filas para eliminar
            if (rowsToDelete.Count == 0)
            {
                return "No rows match the WHERE condition.";
            }

            // Filtrar las filas a eliminar. Usamos un método de comparación explícito para garantizar que las filas coincidan correctamente.
            var rowsRemaining = lines
                .Skip(1)
                .Select(row => row.Split(',').Select(val => val.Trim().Trim('\'')).ToArray())
                .Where(row => !rowsToDelete.Any(r => Enumerable.SequenceEqual(r, row)))
                .ToList();

            // Escribir los cambios en el archivo (reescribir las filas restantes)
            using (var writer = new StreamWriter(tablePath))
            {
                writer.WriteLine(lines[0]); // Escribir los encabezados nuevamente

                foreach (var row in rowsRemaining)
                {
                    writer.WriteLine(string.Join(",", row)); // Escribir las filas restantes
                }
            }

            // Actualizar los índices después de eliminar las filas
            UpdateIndexesAfterDelete(tableName, headers, rowsToDelete);

            return $"{rowsToDelete.Count} row(s) deleted from table '{tableName}'.";
        }



        private static void UpdateIndexesAfterDelete(string tableName, string[] columns, List<string[]> rowsDeleted)
        {
            string systemIndexesPath = Path.Combine(Directory.GetCurrentDirectory(), "SystemIndexes.txt");

            if (File.Exists(systemIndexesPath))
            {
                var indexLines = File.ReadAllLines(systemIndexesPath);
                foreach (var indexLine in indexLines)
                {
                    var parts = indexLine.Split(' ');

                    if (parts.Length != 6)
                    {
                        Console.WriteLine($"Warning: Malformed index entry in SystemIndexes.txt: {indexLine}. Skipping.");
                        continue;
                    }

                    string indexedTableNameWithColumn = parts[2];
                    string indexedTableName = indexedTableNameWithColumn.Split('(')[0];  // Extraer el nombre de la tabla
                    string indexedColumnName = indexedTableNameWithColumn.Split('(')[1].TrimEnd(')');  // Extraer el nombre de la columna
                    string indexType = parts[5]; // Tipo de índice

                    if (indexedTableName == tableName)
                    {
                        int columnIndex = Array.IndexOf(columns, indexedColumnName);  // Comparar solo los nombres

                        if (columnIndex == -1)
                        {
                            Console.WriteLine($"Warning: Column '{indexedColumnName}' not found in table '{tableName}'. Skipping index update.");
                            continue;
                        }

                        foreach (var row in rowsDeleted)
                        {
                            string valueToRemove = row[columnIndex];

                            // Eliminar el valor de los índices correctos
                            if (indexType.ToUpper() == "BTREE" && indexes.TryGetValue(indexedColumnName, out var btreeObj) && btreeObj is BTree btree)
                            {
                                Console.WriteLine($"Removing value '{valueToRemove}' from BTREE index for column '{indexedColumnName}' in table '{tableName}'.");
                                btree.Delete(valueToRemove);  // Eliminar el valor del índice BTREE
                            }
                            else if (indexType.ToUpper() == "BST" && indexes.TryGetValue(indexedColumnName, out var bstObj) && bstObj is BinarySearchTree bst)
                            {
                                Console.WriteLine($"Removing value '{valueToRemove}' from BST index for column '{indexedColumnName}' in table '{tableName}'.");
                                bst.Delete(valueToRemove);  // Eliminar el valor del índice BST
                            }
                        }
                    }
                }
            }
        }


        //===================================================================================================================================================
        private static bool ApplyWhereOperator(string columnValue, string operatorType, string value, bool isNumeric)
        {
            // Eliminar comillas simples del valor si las tiene
            value = value.Trim('\'');

            // Si la columna es numérica, convertimos ambos valores a enteros para comparar
            if (isNumeric)
            {
                if (!int.TryParse(columnValue, out int intColumnValue) || !int.TryParse(value, out int intValue))
                {
                    throw new Exception("Error: Invalid numeric value.");
                }

                switch (operatorType)
                {
                    case "=":
                        return intColumnValue == intValue;
                    case ">":
                        return intColumnValue > intValue;
                    case "<":
                        return intColumnValue < intValue;
                    case ">=":
                        return intColumnValue >= intValue;
                    case "<=":
                        return intColumnValue <= intValue;
                    case "not":
                        return intColumnValue != intValue;
                    default:
                        throw new Exception("Error: Invalid comparison operator for numeric values.");
                }
            }
            else
            {
                // Comparaciones para cadenas de texto
                switch (operatorType)
                {
                    case "=":
                        return columnValue == value;
                    case ">":
                        return string.Compare(columnValue, value) > 0;
                    case "<":
                        return string.Compare(columnValue, value) < 0;
                    case "like":
                        return columnValue.Contains(value);
                    case "not":
                        return columnValue != value;
                    default:
                        throw new Exception("Error: Invalid comparison operator.");
                }
            }
        }


    }
}
