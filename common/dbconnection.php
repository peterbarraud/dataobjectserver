<?php

class MySQLConnection
{
    public static function Open()
    {
  		$connectionstringparts = self::getconnectiondetails();
  		return new mysqli($connectionstringparts['host'],$connectionstringparts['username'],$connectionstringparts['password'],$connectionstringparts['dbname']);
    }
    public static function Close($conn)
    {
        $conn->close();
    }
	
	private static function getconnectiondetails()
	{
		$ret_val = array();
		$dbname = 'pbdotcom';
		file_put_contents('getconnectiondetails','');
		if ($_SERVER['SERVER_NAME'] == 'localhost') {
			$ret_val['host'] = 'localhost';
			$ret_val['username'] = 'root';
			$ret_val['password'] = '';
			$ret_val['dbname'] = $dbname;
		}
		else {
			$ret_val['host'] = 'localhost';
			$ret_val['username'] = 'gapeterb1';
			$ret_val['password'] = 'danielb07';
			$ret_val['dbname'] = 'peterbdotin';
		}
				

		return $ret_val;
	}
}
?>