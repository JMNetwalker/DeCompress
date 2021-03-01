#----------------------------------------------------------------
# Application: Take compressed data and uncompress locally.
#----------------------------------------------------------------

#----------------------------------------------------------------
#Parameters 
#----------------------------------------------------------------
param($server = "", #ServerName parameter to connect 
      $user = "", #UserName parameter  to connect
      $passwordSecure = "", #Password Parameter  to connect
      $Db = "" )#DBName Parameter  to connect) #Folder Paramater to save the csv files 

Function Decompress($Data)
{
 $compressedStream = New-Object System.IO.MemoryStream(,$Data)
 $ZipStream = New-Object System.IO.Compression.GZipStream($compressedStream, [IO.Compression.CompressionMode]::Decompress)
 $resultStream = New-Object System.IO.MemoryStream
 $zipStream.CopyTo($resultStream)
 return $resultStream.ToArray()
}
       
Function UnZip($byteArray)
{
  $sB = New-Object System.Text.StringBuilder
  for($i=0;$i -le $byteArray.Count; $i++)
  {
   $sB.Append( [char] ($byteArray[$i]) )
  }
  return $sB
}

#----------------------------------------------------------------
#Function to connect to the database using a retry-logic
#----------------------------------------------------------------

Function GiveMeConnectionSource()
{ 
  for ($i=1; $i -lt 10; $i++)
  {
   try
    {
      logMsg( "Connecting to the database...Attempt #" + $i) (1)
      $SQLConnection = New-Object System.Data.SqlClient.SqlConnection 
      $SQLConnection.ConnectionString = "Server="+$server+";Database="+$Db+";User ID="+$user+";Password="+$password+";Connection Timeout=60" 
      $SQLConnection.Open()
      logMsg("Connected to the database...") (1)
      return $SQLConnection
      break;
    }
  catch
   {
    logMsg("Not able to connect - Retrying the connection..." + $Error[0].Exception) (2)
    Start-Sleep -s 5
   }
  }
}

#--------------------------------
#Log the operations
#--------------------------------
function logMsg
{
    Param
    (
         [Parameter(Mandatory=$true, Position=0)]
         [string] $msg,
         [Parameter(Mandatory=$false, Position=1)]
         [int] $Color
    )
  try
   {
    $Fecha = Get-Date -format "yyyy-MM-dd HH:mm:ss"
    $msg = $Fecha + " " + $msg
    $Colores="White"
    If($Color -eq 1 )
     {
      $Colores ="Cyan"
     }
    If($Color -eq 3 )
     {
      $Colores ="Yellow"
     }

     if($Color -eq 2)
      {
        Write-Host -ForegroundColor White -BackgroundColor Red $msg 
      } 
     else 
      {
        Write-Host -ForegroundColor $Colores $msg 
      } 


   }
  catch
  {
    Write-Host $msg 
  }
}

#--------------------------------
#Validate Param
#--------------------------------
function TestEmpty($s)
{
if ([string]::IsNullOrWhitespace($s))
  {
    return $true;
  }
else
  {
    return $false;
  }
}

try
{
Clear

#--------------------------------
#Check the parameters.
#--------------------------------

if (TestEmpty($server)) { $server = read-host -Prompt "Please enter a Server Name" }
if (TestEmpty($user))  { $user = read-host -Prompt "Please enter a User Name"   }
if (TestEmpty($passwordSecure))  
    {  
    $passwordSecure = read-host -Prompt "Please enter a password"  -assecurestring  
    $password = [Runtime.InteropServices.Marshal]::PtrToStringAuto([Runtime.InteropServices.Marshal]::SecureStringToBSTR($passwordSecure))
    }
else
    {$password = $passwordSecure} 
if (TestEmpty($Db))  { $Db = read-host -Prompt "Please enter a Database Name"  }

#--------------------------------
#Run the process
#--------------------------------

logMsg("Executing the query to obtain the tables of query store..")  (1)

   $SQLConnectionSource = GiveMeConnectionSource #Connecting to the database.
   if($SQLConnectionSource -eq $null)
    { 
     logMsg("It is not possible to connect to the database") (2)
     exit;
    }

  $sB = New-Object System.Text.StringBuilder
  $SQLCommandExiste = New-Object System.Data.SqlClient.SqlCommand
  $SQLCommandExiste.CommandTimeout = 60
  $SQLCommandExiste.Connection=$SQLConnectionSource
  $SQLCommandExiste.CommandText = "SELECT TOP 20 ID,COMPRESS(NAME1) AS NAME1, COMPRESS(NAME2) AS NAME2,COMPRESS(NAME3) AS NAME3, COMPRESS(NAME4) AS NAME4, COMPRESS(NAME5) AS NAME5 FROM Destination ORDER BY ID"
  $Reader = $SQLCommandExiste.ExecuteReader(); #Executing the Recordset
  while($Reader.Read())
   {
    $DataS = Decompress($Reader.GetSqlBytes(1).Value)
    for($i=0;$i -le $DataS.Count; $i++)
    {
      $sB.Append( [char] ($DataS[$i]) ) | Out-Null 
    }
     logMsg("Data - " + $Reader.GetSqlInt32(0) ) (1) 
     logMsg("Data - " + $sB.ToString().Substring(1,50) ) (1)
   }
   logMsg("Closing the recordset") (1)
   $Reader.Close();
   
   logMsg("Data Collector Script was executed correctly")  (1)
}
catch
  {
    logMsg("Data Collector Script was executed incorrectly ..: " + $Error[0].Exception) (2)
  }
finally
{
   logMsg("Data Collector Script finished - Check the previous status line to know if it was success or not") (2)
} 



