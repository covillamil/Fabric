[DefaultOutputDestinationSettings = [DestinationDefinition = [Kind = "Reference", QueryName = "DefaultDestination", IsNewTarget = true], UpdateMethod = [Kind = "Replace"], DestinationTypeSettings = [Kind = "Table"]], StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared DefaultDestination = Lakehouse.Contents([EnableFolding = false]){[workspaceId = "55ce2ee2-6ba8-444a-af5d-9ef08b8195db"]}[Data]{[lakehouseId = "c01939a0-bfb0-41ca-9644-670d14dfe6ce"]}[Data];
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "Sales_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared Sales = let
  Source = Csv.Document(Web.Contents("https://liondata-my.sharepoint.com/personal/covillamil_liondata_com_co/Documents/Aplicaciones/Microsoft Power Query/Uploaded Files/sales 1.csv"), [Delimiter = ",", Columns = 9, Encoding = 65001, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(Source, [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Promoted headers", {{"SalesOrderNumber", type text}, {"SalesOrderLineNumber", Int64.Type}, {"OrderDate", type date}, {"CustomerName", type text}, {"EmailAddress", type text}, {"Item", type text}, {"Quantity", Int64.Type}, {"UnitPrice", type number}, {"TaxAmount", type number}}),
  #"Removed columns" = Table.RemoveColumns(#"Changed column type", {"EmailAddress"}),
  #"Choose columns" = Table.SelectColumns(#"Removed columns", {"SalesOrderNumber", "SalesOrderLineNumber", "OrderDate", "Quantity"})
in
  #"Choose columns";
shared Sales_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "55ce2ee2-6ba8-444a-af5d-9ef08b8195db"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "c01939a0-bfb0-41ca-9644-670d14dfe6ce"]}[Data],
  TableNavigation = Navigation_2{[Id = "SalesDemoDataFlow", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
