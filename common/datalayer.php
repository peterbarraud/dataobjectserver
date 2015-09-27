<?php

class DataLayer {
  private $conn = null;
  public function OpenConnection ()
  {
	  $json = json_decode(file_get_contents('dataobjectserver/common/dbmetadata.json'), true);
	  $this->conn = new mysqli(trim($json['dbserver']),trim($json['dbuser']),trim($json['dbpwd']),trim($json['dbname']));
  }
  public function CloseConnection()
  {
    $this->conn->Close();
  }  
  public function GetObjectCollection($object,$className,$classAttrValuePairs=null,$sortby=null,$getrelatedobjectcollection=0) {

	$select_sql = "select id from $className";
	if ($classAttrValuePairs) {
		$select_sql .= ' where ';
		foreach ($classAttrValuePairs as $key => $value) {
			$select_sql .= "$key = '$value' and ";
		}
		$select_sql = rtrim($select_sql,'and ');	//strip out the last appended add
	}
	if ($sortby) {
		$select_sql .= ' order by ';
		foreach ($sortby as $key => $value) {
			$select_sql .= "$key $value,";
		}
		$select_sql = rtrim($select_sql,',');	//strip out the last appended comma
	}
	if ($result = $this->conn->query($select_sql)){
		while ($row = $result->fetch_array()){
			//the object constructor also takes an argument to get the related object collections
			//we can possibly create another method to get the object collection along with the related object collections
			array_push($object->Items,new $className($row[0],$getrelatedobjectcollection));
		}
		$object->Length = sizeof($object->Items);
	}
  }
  public function GetObjectProperties($object,$getrelatedobjectcollection = 0)
  {
	$id = $object->id ?: 'null';  //if else super shortcut
	$select_sql = 'select * from ' . get_class($object) . ' where id = ' . $id;
    if ($result = $this->conn->query($select_sql)) {
		$row = $result->fetch_assoc();
		while ($fieldinfo = $result->fetch_field()) {
		  $object->{$fieldinfo->name} = $row[$fieldinfo->name];
		}
		if ($getrelatedobjectcollection) {
			$this->getRelatedObjectCollections($object);
		}
	}
  }
  private function getRelatedObjectCollections($object) {
	  $id = $object->id ?: 'null';
	  $this_table_name = get_class($object);
	  //get the mapping tables
	  $mapping_tables = DataLayer::getMappingTables($object,$this->conn);
	  foreach ($mapping_tables as $mapping_table_name) {
		  $this_table_fk_col_name = '';
		  $related_table_name = '';
		  $related_table_fk_col_name = '';
		  //from each mapping table, get the related table info
		  $relatedTablesInfo = DataLayer::getRelatedTablesInfo($mapping_table_name,$this->conn);
		  foreach ($relatedTablesInfo as $row) {
			  if ($row['referenced_table_name'] == $this_table_name) {
				  $this_table_fk_col_name = $row['column_name'];
			  }
			  else {
				  $related_table_name = $row['referenced_table_name'];
				  $related_table_fk_col_name = $row['column_name'];
			  }
		  }
		  //if one-to-one relationship
		  if ($related_table_name == '') {
			  $related_table_name = $mapping_table_name;
			  //select userreply.id from userreply, post where userreply.postid = post.id;
			  $select_sql = "select " . $related_table_name . ".id from " . $related_table_name  . "," . $this_table_name  . " where " . $related_table_name . "." . $this_table_fk_col_name .  " = " . $this_table_name . ".id and " . $this_table_name . ".id = " . $id . ";";
		  }
		  //if one-to-many relationship
		  else {
			  $select_sql = "select " . $related_table_name . ".id from " . $related_table_name  . "," . $this_table_name  . "," . $mapping_table_name . " where " . $related_table_name . ".id = " . $mapping_table_name . "." . $related_table_fk_col_name . " and " . $this_table_name . ".id = " . $mapping_table_name . "." .$this_table_fk_col_name . " and " . $this_table_name . ".id = " . $id . " group by " . $related_table_name . ".id ;";
		  }
		  if ($related_id_result = $this->conn->query($select_sql)) {
			  //create the related object collection property
			  //even if there are not defined relateds
			  //this way we at leastr get an empty array
			  require_once 'dataobjectserver/' . $related_table_name . '.php';
			  $className = $related_table_name;
			  $object->{$className} = array();
			  if ($related_id_result->num_rows) {
				  while ($row = $related_id_result->fetch_assoc()) {
					  //now when we call GetObjectProperties we will not request for related objects
					  //this means that we will only go down one level when creating objects
					  //this will prevent going too t
					  //but more importantly, avoid going into an infinite loop for co-related objects
					  $instance = new $className($row['id']);
					  array_push($object->{$className},$instance);
				  }
			  }
		  }
	  }
  }
  public function SaveObjectProperties($object)
  {
    $id = $object->id ?: 'null';  //if else super shortcut
    $this_table_name = get_class($object);
    $select_sql = 'select * from ' . $this_table_name . ' where id = ' . $id;
    $object->pagename = null;
    if ($result = $this->conn->query($select_sql)) {
		$columns = array();
		while ($fieldinfo = $result->fetch_field()) {
			array_push($columns, $fieldinfo);
		}
		$insert_update_on_duplicate = 'insert into ' . $this_table_name . '(';
		for ($i=0;$i<sizeof($columns);$i++) {
			$insert_update_on_duplicate .=  $columns[$i]->name;
			if ($i < sizeof($columns) - 1) {
				$insert_update_on_duplicate .= ',';
			}
		}
		$insert_update_on_duplicate .= ')';
		$insert_update_on_duplicate .= ' values(';
		for ($i=0;$i<sizeof($columns);$i++) {
			$insert_update_on_duplicate .= DataLayer::prettifydatavalue($object->{$columns[$i]->name},$columns[$i]);
			if ($i < sizeof($columns) - 1) {
				$insert_update_on_duplicate .= ',';
			}
		}
		$insert_update_on_duplicate .= ')';
		$insert_update_on_duplicate .= ' on duplicate key update ';
		for ($i=0;$i<sizeof($columns);$i++) {
			$insert_update_on_duplicate .= $columns[$i]->name . ' = ' . DataLayer::prettifydatavalue($object->{$columns[$i]->name},$columns[$i]);
			if ($i < sizeof($columns) - 1) {
				$insert_update_on_duplicate .= ',';
			}
		}
		$insert_update_on_duplicate .= ';';
		if ($this->conn->query($insert_update_on_duplicate)) {
			//$this->NewId = $this->conn->insert_id;
			$object->id = $this->conn->insert_id;
		}
		else {
			throw new Exception ($this->conn->error);
		}
		$this->setRelatedObjectCollections($object);
	}
  }
  private function setRelatedObjectCollections($object) {
	  $this_table_name = get_class($object);
	  //get the mapping tables
	  $mapping_tables = DataLayer::getMappingTables($object,$this->conn);
	  foreach ($mapping_tables as $mapping_table_name) {
		  $this_table_fk_col_name = '';
		  $related_table_name = '';
		  $related_table_fk_col_name = '';
		  //from each mapping table, get the related table info
		  $relatedTablesInfo = DataLayer::getRelatedTablesInfo($mapping_table_name,$this->conn);
		  foreach ($relatedTablesInfo as $row) {
			  if ($row['referenced_table_name'] == $this_table_name) {
				  $this_table_fk_col_name = $row['column_name'];
			  }
			  else {
				  $related_table_name = $row['referenced_table_name'];
				  $related_table_fk_col_name = $row['column_name'];
			  }
		  }
		  //this system does not support one-to-one table relations
		  //but just to handle the one-off case of one-to-one relations
		  
		  if ($related_table_name) {
			  //now there's no way to know from the current related ids if the user has deleted a related object
			  //so right now the only way we can think of to remove all entries for the main table id
			  $clear_relations = "delete from $mapping_table_name where $this_table_fk_col_name = $object->id;";
			  if ($this->conn->query($clear_relations)) {
				//then re-insert the relations. for all relations
				  foreach ($object->{$related_table_name} as $relatedObject) {
					  $update_relations = "insert into $mapping_table_name ($this_table_fk_col_name,$related_table_fk_col_name) values($object->id,$relatedObject->id);";
					  if ($this->conn->query($update_relations)) {
					  }
					  else {
						  throw new Exception($this->conn->error,$this->conn->errno);
					  }
				  }
			  }
			  else {
				  throw new Exception($this->conn->error,$this->conn->errno);
			  }
		  }
	  }
  }
  public function DeleteObject($object)
  {
    $delete_sql = 'delete from ' . get_class($object) . ' where id = ' . $object->id;
    $result = $this->conn->query($delete_sql);
  }
  
  private static function fieldisnotnumeric($fieldinfo) {
	  return $fieldinfo->type != 1 && $fieldinfo->type != 2 && $fieldinfo->type != 3 &&
				$fieldinfo->type != 4 && $fieldinfo->type != 5 && $fieldinfo->type != 8 &&
				$fieldinfo->type != 9 && $fieldinfo->type != 246;
  }
  private static function prettifydatavalue($value,$fieldinfo) {
	$retval = $value;
	if ($value) {
		if (DataLayer::fieldisnotnumeric($fieldinfo)) {
			if ($fieldinfo->type == 7 && $value == 'now()') {
				$retval = $value;
			}
			else {
				$retval = "'" . $value . "'";
			}
		}
	}
	else {
		$retval = 'null';
	}
	return $retval;
  }
  private static function getMappingTables($object,$conn) {
	  $select_sql = "select table_name from information_schema.key_column_usage where referenced_table_name = '" . get_class($object) . "'";
	  $mapping_tables = array();
	  if ($result = $conn->query($select_sql)) {
		  while ($row = $result->fetch_assoc()) {
			  array_push($mapping_tables,$row['table_name']);
		  }
	  }
	  return $mapping_tables;
  }
  private static function getRelatedTablesInfo($mapping_table_name,$conn) {
	  $relatedTablesInfo = array();
	  $select_sql = "select referenced_table_name, column_name,referenced_column_name from information_schema.key_column_usage where table_name = '" . $mapping_table_name . "' and referenced_table_name is not null;";
	  if ($result = $conn->query($select_sql)) {
		  while ($row = $result->fetch_assoc()) {
			  array_push($relatedTablesInfo,$row);
		  }
	  }
	  return $relatedTablesInfo;
  }
  
}
  
?>
