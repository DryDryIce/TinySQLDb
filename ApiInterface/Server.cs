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
                else if (request.StartsWith("DROP TABLE"))
                {
                    var tableName = request.Split(' ')[2].TrimEnd(';');
                    return DropTable(tableName);
                }
                else if (request.Trim().Equals("SHOW DATABASES", StringComparison.OrdinalIgnoreCase))
                {
                    return ShowDatabases();
                }
                else if (request.StartsWith("CREATE INDEX"))
                {
                    return CreateIndex(request);
                }
                else if (request.StartsWith("INSERT INTO"))
                {
                    return InsertIntoTable(request);
                }
                else if (request.StartsWith("UPDATE", StringComparison.OrdinalIgnoreCase))
                {
                    return UpdateTable(request);  // Manejar la sentencia UPDATE
                }
                else if (request.Trim().Equals("SHOW DATABASE PATHS", StringComparison.OrdinalIgnoreCase))
                {
                    return ShowDatabasePaths();
                }
                else if (request.StartsWith("SELECT", StringComparison.OrdinalIgnoreCase))
                {
                    return SelectFromTable(request);
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

                // Agregar información al System Catalog (SystemDatabases.txt)
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
        private static string ShowDatabases()
        {
            string systemCatalogPath = Path.Combine(Directory.GetCurrentDirectory(), "SystemDatabases.txt");

            // Si el archivo no existe, devolver un mensaje
            if (!File.Exists(systemCatalogPath))
            {
                return "No databases found.";
            }

            // Leer las bases de datos
            var databases = File.ReadAllLines(systemCatalogPath);

            // Si el archivo está vacío o no tiene bases de datos, devolver un mensaje
            if (databases.Length == 0 || string.IsNullOrWhiteSpace(string.Join("", databases)))
            {
                return "No databases found.";
            }

            // Devolver la lista de bases de datos
            return "Databases:\n" + string.Join("\n", databases.Where(db => !string.IsNullOrWhiteSpace(db)));
        }

        // Muestra los PATHs de las DATABASEs
        private static string ShowDatabasePaths()
        {
            string systemCatalogPath = Path.Combine(Directory.GetCurrentDirectory(), "SystemDatabases.txt");

            // Si el archivo no existe, devolver un mensaje
            if (!File.Exists(systemCatalogPath))
            {
                return "No databases found.";
            }

            // Leer las bases de datos
            var databases = File.ReadAllLines(systemCatalogPath);

            // Si el archivo está vacío o no tiene bases de datos, devolver un mensaje
            if (databases.Length == 0 || string.IsNullOrWhiteSpace(string.Join("", databases)))
            {
                return "No databases found.";
            }

            // Obtener las rutas completas de cada base de datos
            var databasePaths = databases.Where(db => !string.IsNullOrWhiteSpace(db))
                                         .Select(db => Path.Combine(Directory.GetCurrentDirectory(), db));

            // Devolver las rutas completas
            return "Database Paths:\n" + string.Join("\n", databasePaths);
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
            if (string.IsNullOrEmpty(currentDatabase))
            {
                return "No database selected. Please use 'SET DATABASE <db-name>' first.";
            }

            var match = Regex.Match(request, @"CREATE\s+INDEX\s+(\w+)\s+ON\s+(\w+)\((\w+)\)\s+OF\s+TYPE\s+(\w+);?", RegexOptions.IgnoreCase);

            if (!match.Success)
            {
                return "Error: Invalid CREATE INDEX command. Expected format: CREATE INDEX <index_name> ON <table_name>(<column_name>) OF TYPE <index_type>";
            }

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

            // Verificar si la columna existe
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

            // Registrar el índice en SystemIndexes.txt
            string systemIndexesPath = Path.Combine(Directory.GetCurrentDirectory(), "SystemIndexes.txt");
            File.AppendAllText(systemIndexesPath, $"{indexName} ON {tableName}({columnName}) OF TYPE {indexType}{Environment.NewLine}");

            return $"Index '{indexName}' created successfully on column '{columnName}' of table '{tableName}' with type '{indexType}'.";
        }

        private static void LoadIndexesOnStartup()
        {
            string systemIndexesPath = Path.Combine(Directory.GetCurrentDirectory(), "SystemIndexes.txt");

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

                    if (parts.Length != 6) // Ajuste a 6 partes
                    {
                        Console.WriteLine($"Error: Malformed index entry: '{indexLine}'");
                        continue;
                    }

                    string indexName = parts[0];
                    string tableNameWithColumn = parts[2];
                    string tableName = tableNameWithColumn.Split('(')[0];  // Extraer la tabla
                    string columnName = tableNameWithColumn.Split('(')[1].TrimEnd(')');  // Extraer la columna
                    string indexType = parts[5]; // Tipo de índice

                    // Recrear el índice en memoria
                    if (indexType.ToUpper() == "BTREE")
                    {
                        Console.WriteLine($"Rebuilding BTREE index '{indexName}' for table '{tableName}', column '{columnName}'.");
                        indexes[columnName] = new BTree(3); // Ajusta el grado según sea necesario
                    }
                    else if (indexType.ToUpper() == "BST")
                    {
                        Console.WriteLine($"Rebuilding BST index '{indexName}' for table '{tableName}', column '{columnName}'.");
                        indexes[columnName] = new BinarySearchTree();
                    }
                    else
                    {
                        Console.WriteLine($"Error: Invalid index type '{indexType}' for index '{indexName}'");
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

            // Actualizar los índices asociados
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
                    string indexedTableName = indexedTableNameWithColumn.Split('(')[0];  // Extraer la tabla
                    string indexedColumnName = indexedTableNameWithColumn.Split('(')[1].TrimEnd(')');  // Extraer la columna
                    string indexType = parts[5]; // Tipo de índice

                    if (indexedTableName == tableName)
                    {
                        int columnIndex = Array.IndexOf(columns, indexedColumnName);

                        if (columnIndex == -1)
                        {
                            Console.WriteLine($"Warning: Column '{indexedColumnName}' not found in table '{tableName}'. Skipping index check.");
                            continue;
                        }

                        string valueToCheck = values[columnIndex];

                        if (indexes.TryGetValue(indexedColumnName, out var indexObject))
                        {
                            if (indexType.ToUpper() == "BTREE" && indexObject is BTree btree)
                            {
                                if (btree.Search(valueToCheck))
                                {
                                    return true; // Duplicado encontrado
                                }
                            }
                            else if (indexType.ToUpper() == "BST" && indexObject is BinarySearchTree bst)
                            {
                                if (bst.Search(valueToCheck))
                                {
                                    return true; // Duplicado encontrado
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
                    string indexedTableName = indexedTableNameWithColumn.Split('(')[0];  // Extraer la tabla
                    string indexedColumnName = indexedTableNameWithColumn.Split('(')[1].TrimEnd(')');  // Extraer la columna
                    string indexType = parts[5]; // Tipo de índice

                    if (indexedTableName == tableName)
                    {
                        int columnIndex = Array.IndexOf(columns, indexedColumnName);

                        if (columnIndex == -1)
                        {
                            Console.WriteLine($"Warning: Column '{indexedColumnName}' not found in table '{tableName}'. Skipping index update.");
                            continue;
                        }

                        string newValue = values[columnIndex];

                        if (indexType.ToUpper() == "BTREE")
                        {
                            Console.WriteLine($"Updating BTREE index for column '{indexedColumnName}' in table '{tableName}' with new value '{newValue}'.");
                        }
                        else if (indexType.ToUpper() == "BST")
                        {
                            Console.WriteLine($"Updating BST index for column '{indexedColumnName}' in table '{tableName}' with new value '{newValue}'.");
                        }
                    }
                }
            }
        }




        //===================================================================================================================================================
        private static string SelectFromTable(string request)
        {
            Console.WriteLine($"DEBUG: Received SELECT command: {request}");

            var match = Regex.Match(request, @"SELECT\s+(\*|[\w\s,]+)\s+FROM\s+(\w+)(?:\s+WHERE\s+(.+?))?(?:\s+ORDER\s+BY\s+(\w+)(?:\s+(ASC|DESC))?)?\s*;", RegexOptions.IgnoreCase);

            if (!match.Success)
            {
                return "Error: Invalid SELECT command.";
            }

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

            // Obtener los encabezados de la tabla
            string[] headers = lines[0].Split(',').Select(h => h.Trim()).ToArray();

            // Filtrar por WHERE si existe
            List<string[]> result = lines.Skip(1) // Ignorar encabezado
                .Select(line => line.Split(',').Select(val => val.Trim()).ToArray()) // Convertir a array
                .Where(row => ApplyWhereClause(row, headers, whereClause)) // Aplicar filtro WHERE
                .ToList();

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

            // Mostrar los resultados
            string output = string.Join(",", headers) + Environment.NewLine; // Encabezados
            output += string.Join(Environment.NewLine, result.Select(row => string.Join(",", row))); // Filas
            return output;
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

        private static int Partition(List<string[]> result, int columnIndex, int left, int right, bool desc)
        {
            string[] pivot = result[right];
            int i = left - 1;

            for (int j = left; j < right; j++)
            {
                bool comparison = desc
                    ? string.Compare(result[j][columnIndex], pivot[columnIndex]) > 0
                    : string.Compare(result[j][columnIndex], pivot[columnIndex]) < 0;

                if (comparison)
                {
                    i++;
                    (result[i], result[j]) = (result[j], result[i]);
                }
            }

            (result[i + 1], result[right]) = (result[right], result[i + 1]);
            return i + 1;
        }

        // Función para aplicar la cláusula WHERE
        private static bool ApplyWhereClause(string[] row, string[] headers, string whereClause)
        {
            if (string.IsNullOrEmpty(whereClause))
            {
                return true; // No hay WHERE, incluir todas las filas
            }

            // Parsear WHERE
            var match = Regex.Match(whereClause, @"(\w+)\s*(=|>|<|like|not)\s*(.+)", RegexOptions.IgnoreCase);
            if (!match.Success)
            {
                return false; // Cláusula WHERE no válida
            }

            string columnName = match.Groups[1].Value;
            string operatorType = match.Groups[2].Value;
            string value = match.Groups[3].Value.Trim('\'');

            int columnIndex = Array.IndexOf(headers, columnName);
            if (columnIndex == -1)
            {
                return false; // Columna no encontrada
            }

            string cellValue = row[columnIndex];

            // Aplicar el operador
            return operatorType switch
            {
                "=" => cellValue == value,
                ">" => string.Compare(cellValue, value, StringComparison.OrdinalIgnoreCase) > 0,
                "<" => string.Compare(cellValue, value, StringComparison.OrdinalIgnoreCase) < 0,
                "like" => cellValue.Contains(value, StringComparison.OrdinalIgnoreCase),
                "not" => cellValue != value,
                _ => false
            };
        }

        //===================================================================================================================================================
        private static string UpdateTable(string request)
        {
            Console.WriteLine($"DEBUG: Received UPDATE command: {request}");

            var match = Regex.Match(request, @"UPDATE\s+(\w+)\s+SET\s+(\w+)\s*=\s*(.+?)(?:\s+WHERE\s+(.+?))?\s*;", RegexOptions.IgnoreCase);

            if (!match.Success)
            {
                return "Error: Invalid UPDATE command.";
            }

            string tableName = match.Groups[1].Value.Trim();
            string columnName = match.Groups[2].Value.Trim();
            string newValue = match.Groups[3].Value.Trim().Trim('\''); // Eliminar las comillas del nuevo valor
            string whereClause = match.Groups[4].Value?.Trim();

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

            // Obtener los encabezados de la tabla
            string[] headers = lines[0].Split(',').Select(h => h.Trim()).ToArray();
            int columnIndex = Array.IndexOf(headers, columnName);
            if (columnIndex == -1)
            {
                return $"Error: Column '{columnName}' not found in table '{tableName}'.";
            }

            // Buscar si existe índice para la columna del WHERE (si aplica)
            List<string[]> result = lines.Skip(1) // Ignorar encabezado
                .Select(line => line.Split(',').Select(val => val.Trim()).ToArray()) // Convertir a array
                .Where(row => ApplyWhereClause(row, headers, whereClause)) // Filtrar filas según WHERE
                .ToList();

            // Actualizar los valores
            for (int i = 0; i < result.Count; i++)
            {
                result[i][columnIndex] = newValue;
            }

            // Escribir los cambios en la tabla
            using (StreamWriter writer = new StreamWriter(tablePath))
            {
                writer.WriteLine(string.Join(",", headers)); // Escribir los encabezados
                foreach (var row in result)
                {
                    writer.WriteLine(string.Join(",", row));
                }
            }

            // Actualizar los índices asociados si el campo actualizado tiene índice
            UpdateIndexesAfterUpdate(tableName, headers, result, columnIndex, newValue);

            return $"Table '{tableName}' updated successfully.";
        }

        // Función para actualizar los índices después de UPDATE
        private static void UpdateIndexesAfterUpdate(string tableName, string[] columns, List<string[]> rows, int columnIndex, string newValue)
        {
            string systemIndexesPath = Path.Combine(Directory.GetCurrentDirectory(), "SystemIndexes.txt");

            if (File.Exists(systemIndexesPath))
            {
                var indexLines = File.ReadAllLines(systemIndexesPath);
                foreach (var indexLine in indexLines)
                {
                    var parts = indexLine.Split(' ');

                    // Asegurarse de que la entrada tenga 6 partes
                    if (parts.Length != 6)
                    {
                        Console.WriteLine($"Warning: Malformed index entry in SystemIndexes.txt: {indexLine}. Skipping.");
                        continue;
                    }

                    string indexedTableNameWithColumn = parts[2];
                    string indexedTableName = indexedTableNameWithColumn.Split('(')[0];  // Extraer la tabla
                    string indexedColumnName = indexedTableNameWithColumn.Split('(')[1].TrimEnd(')');
                    string indexType = parts[5]; // Sexta parte

                    if (indexedTableName == tableName)
                    {
                        int indexColumnIndex = Array.IndexOf(columns, indexedColumnName);

                        // Validar si la columna actualizada tiene índice
                        if (indexColumnIndex == columnIndex)
                        {
                            if (indexType.ToUpper() == "BTREE")
                            {
                                // Actualizar índice BTREE
                                Console.WriteLine($"Updating BTREE index for column '{indexedColumnName}' in table '{tableName}' with new value '{newValue}'.");
                            }
                            else if (indexType.ToUpper() == "BST")
                            {
                                // Actualizar índice BST
                                Console.WriteLine($"Updating BST index for column '{indexedColumnName}' in table '{tableName}' with new value '{newValue}'.");
                            }
                        }
                    }
                }
            }
        }



        //===================================================================================================================================================



    }
}
