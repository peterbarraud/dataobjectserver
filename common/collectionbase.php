<?php 
require_once dirname(__FILE__) . '/applicationbase.php';
class collectionbase extends applicationbase {
	public function __construct($classAttrValuePairs=null,$sortby=null,$getrelatedobjectcollection = 0) {
	  require_once dirname(__FILE__) . '/datalayer.php';
	  $className = str_replace('collection','',get_class($this));
	  require_once dirname(__FILE__) . '/../' . $className . '.php';
	  $datalayer = new DataLayer();
	  $datalayer->OpenConnection();
	  $datalayer->GetObjectCollection($this,$className,$classAttrValuePairs,$sortby,$getrelatedobjectcollection);
	  $datalayer->CloseConnection();
	}

	//do we need functions that save / delete / etc an entire collection?
	public function Save()
	{
	}
	public function Delete()
	{
	  require_once dirname(__FILE__) . '/datalayer.php';
	  $datalayer = new DataLayer();
	  $datalayer->DeleteObject($this);
	}
	public $ServerErr = '';
	public $ServerErrNo = 0;
	public $ServerErrType = '';
	public $Items = array();
	public $Length = 0;
}








  


?>
