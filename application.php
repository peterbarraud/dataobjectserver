<?php
class Application
{
	//GetObjectsByClassName: get a collection of objects for the give class
	//$className: Class of objects collection
	//$sortby: provide the sql sort order
	//$getrelatedobjectcollection: Get one level down related object collections
	public function GetObjectsByClassName($className,$sortby=null,$getrelatedobjectcollection=0) {
		require_once dirname(__FILE__) . '/' . $className  . 'collection.php';
		$collectionClassName = $className  . 'collection';
		return new $collectionClassName(null,$sortby,$getrelatedobjectcollection);
	}
	public function GetObjectsByClassNameAndAttributes($className,$classAttrValuePairs,$sortby=null,$getrelatedobjectcollection=0) {
		require_once dirname(__FILE__) . '/' . $className  . 'collection.php';
		$collectionClassName = $className  . 'collection';
		return new $collectionClassName($classAttrValuePairs,$sortby,$getrelatedobjectcollection);
	}
	//$getrelatedobjectcollection: Get one level down related object collections	
	public function GetObjectById($className,$id,$getrelatedobjectcollection=0) {
		require_once dirname(__FILE__) . '/' . $className  . '.php';
		return new $className($id,$getrelatedobjectcollection);
	}
	//utility function used if you have a well formed object BUT in json format
	//so we will use a cast function to convert it to a well formed php object
	public function GetObjectForJSON($jsonobject,$className) {
		require_once dirname(__FILE__) . '/' . $className  . '.php';
		//TODO: where is the credit for the following code line
		return unserialize(sprintf('O:%d:"%s"%s',strlen($className),$className,strstr(strstr(serialize($jsonobject), '"'), ':')));
	}
	//The SaveObject and DeleteObject methods are specializations of the generic Execute method
	//We've added these here as convenience methods
	//By default, the dataobjectserver serves up the complete CRUD, everything else goes through the Execute
	public function SaveObject($object) {
		require_once dirname(__FILE__) . '/' . get_class($object)  . '.php';
		$object->Save();
	}
	public function DeleteObject($object) {
		require_once dirname(__FILE__) . '/' . get_class($object)  . '.php';
		$object->Delete();
	}
	public function Execute($object,$callback) {
		require_once dirname(__FILE__) . '/' . get_class($object)  . '.php';
		//but first make sure that callback method exists
		if (method_exists($object,$callback)) {
			$object->$callback();
		}
		else {
			$object->ServerErr = 'Method: ' . $callback . ' has not been defined for Class: ' . get_class($object) . '.';
			$object->ServerErrNo = 9999;
		}
	}
	
	//singleton methods: @http://www.phptherightway.com/pages/Design-Patterns.html
    private static $instance;
    public static function getInstance(){if (null === static::$instance) {static::$instance = new static();}return static::$instance;}
    protected function __construct(){}
    private function __clone(){}
    private function __wakeup(){}
}

?>
