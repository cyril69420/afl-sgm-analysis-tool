let
    Source = Folder.Files("C:\\Users\\Ethan\\SGM Agent Project\\gold"),
    KeepCSVs = Table.SelectRows(Source, each Text.EndsWith([Extension], ".csv"))
    // etc.
 in
    KeepCSVs
