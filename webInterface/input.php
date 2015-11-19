<?php
if(isset($_GET['query']))
{
    $input = $_GET['query'];
}
if(isset($_GET['rank']))
{
    $rank=$_GET['rank'];
} else {
    $rank=3;
}
if($rank == 1){
    shell_exec('sh boolean.sh"'.$input.'"');
} elseif($rank == 2) {
    shell_exec('sh ranked.sh"'.$input.'"');
} else {
    shell_exec('sh okapi.sh"'.$input.'"');
}
shell_exec('sh okapi.sh');
sleep(300);
shell_exec('sh copy.sh');
?>
<html>
<head>
<title>
Search Results
</title>
</head>
<body>
<h3>Search Results</h3>
<?php
$fp = fopen("outputfiles.txt","r");
if ($fp)
{
  $flag = 0;
  echo "<ol>";
  while ($line = fgets($fp))
  {
    list($part1, $part2) = explode(";",$line);
    if($flag == 0)
    {
    echo "<li><a href=text/$part1>$part2 </a></li>";
    $flag = 0;
    }
    else
    {
	echo "\n $line \n\n\n ";
        $flag = 0;
    }
  }
  echo "</ul>";
}

?>

</body>
</html>
