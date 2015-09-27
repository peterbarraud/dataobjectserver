<?php  
require_once dirname(__FILE__) . '/applicationbase.php';
		require_once dirname(__FILE__) . '/datalayer.php';
class objectbase extends applicationbase {
	public function __construct($id,$getrelatedobjectcollection = 0) {
		$this->id = $id;
		$datalayer = new DataLayer();
		$datalayer->OpenConnection();
		$datalayer->GetObjectProperties($this,$getrelatedobjectcollection);
		$datalayer->CloseConnection();
	}

	public function Save()
	{
	  $datalayer = new DataLayer();
	  $datalayer->OpenConnection();
	  try {
		  $datalayer->SaveObjectProperties($this);
		  $log = new Logger('saveobjectbase');
		  $log->Write($datalayer->NewId);
		  //$this->id = $datalayer->NewId;
	  }
	  catch (Exception $e) {
		  $this->ServerErr = $e->getMessage();
		  $this->ServerErrNo = $e->getCode();
	  }
	  $datalayer->CloseConnection();
	}
	public function Delete()
	{
	  $datalayer = new DataLayer();
	  $datalayer->OpenConnection();
	  try {
		  $datalayer->DeleteObject($this);
	  }
	  catch (Exception $e) {
		  $this->ServerErr = $e->getMessage();
		  $this->ServerErrNo = $e->getCode();
	  }
	  $datalayer->CloseConnection();
	}
	public function __toString() {
		$retval = '';
		$object_vars = get_object_vars ($this);
		while ( list ($key, $value) = each ($object_vars) ) {	//nice hash iterator code
			$print_val = $value;
			if (isset($print_val)) {
				$print_val = is_array ($value) ? sizeof($value) : $value;
			
			}
			else {
				$print_val = 'NULL';
			}
			$retval .= $key . '>> ' . $print_val . "\n";
		}
		return $retval;
	}
	public $ServerErr = '';
	public $ServerErrNo = 0;
	public $ServerErrType = '';
	public $NewId = 0;
}



?>
