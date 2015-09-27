<?php

class Logger {
  private $fileisopen = 0;
  public function __construct($logfilename = null) {
    if ($logfilename == null) {	//use the name of the current function
      $logfilename = debug_backtrace()[1]['function'] . '.log';
    }
    else {
		if (Logger::endsWith($logfilename,'.log') == false) {
			$logfilename .= '.log';
		}		
	}
    $this->LogFileName = $logfilename;
  }
  public function Write($text = '') {
    file_put_contents($this->LogFileName,$text);
  }
  public function WriteLine($line = '') {
    file_put_contents($this->LogFileName,$line . "\n");
  }
  public function Append($text = '') {
    file_put_contents($this->LogFileName,$text,FILE_APPEND);
  }
  public function AppendLine($line = '') {
    file_put_contents($this->LogFileName,$line . "\n",FILE_APPEND);
  }
  public function Separator() {
    file_put_contents($this->LogFileName,"===================\n",FILE_APPEND);
  }
  //public in case a caller wants to know the file name
  public $LogFileName = null;
  
  
  
  private static function endsWith( $str, $sub ) {
   return ( substr( $str, strlen( $str ) - strlen( $sub ) ) === $sub );
}
}










?>
