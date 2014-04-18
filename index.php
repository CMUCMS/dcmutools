<?php

class HTMLNode
{
  public $tag = '';
  public $id = '';
  public $name = '';
  public $attributes = array();
  public $daughters = array();

  function __construct($tag, $id = '', $name = '', $attributes = NULL, $daughters = NULL)
  {
    $this->tag = $tag;
    $this->attributes = array();

    if($attributes){
      foreach($attributes as $key => $value)
        $this->attributes[$key] = $attributes[$key];
    }
    if($id != '') $this->attributes['id'] = $id;
    if($name != '') $this->attributes['name'] = $name;

    if($daughters !== NULL){
      if(is_array($daughters)){
        foreach($daughters as $value)
          $this->daughters[] = $value;
      }
      else
        $this->daughters[0] = $daughters;
    }
  }

  public function addDaughter($tag, $id = '', $name = '', $attributes = NULL, $daughters = NULL)
  {
    $this->daughters[] = new HTMLNode($tag, $id, $name, $attributes, $daughters);
    return $this->daughters[count($this->daughters) - 1];
  }

  public function generateHTML($indent = 0)
  {
    $html = str_repeat(' ', $indent) . '<' . $this->tag;
    if(count($this->attributes) != 0){
      foreach($this->attributes as $key => $value)
        $html .= ' ' . $key . '=' . '"' . $value . '"';
    }

    if(!is_array($this->daughters))
      $this->daughters = array($this->daughters);

    if(count($this->daughters) == 0)
      $html .= " />\n";
    else{
      $html .= ">\n";
      foreach($this->daughters as $daughter){
        if(is_object($daughter))
          $html .= $daughter->generateHTML($indent + 2);
        else
          $html .= str_repeat(' ', $indent + 2) . $daughter . "\n";
      }
      $html .= str_repeat(' ', $indent) . '</' . $this->tag . ">\n";
    }

    return $html;
  }
}

/////// DELETE TASK DIRECTORY ////////
foreach($_POST as $key => $value){
  if(strpos($key, '_delete') === FALSE) continue;
  if($value != '1') continue;
  system('/bin/rm -rf ' . escapeshellarg(substr($key, 0, strrpos('_delete'))));
  exit(0);
}

/////// GET LIST OF TASKS ////////
$taskList = array();
foreach(scandir('.') as $task){
  if(!is_dir($task)) continue;
  if($task == '.' || $task == '..') continue;
  $stat = stat($task);
  $taskList[$task] = $stat['ctime'];
}
arsort($taskList);
$taskList = array_keys($taskList);

/////// GENERATE HTML /////////

$html = new HTMLNode('html', '', '', array('xmlns' => 'http://www.w3.org/1999/xhtml'));
$head = $html->addDaughter('head');
$head->addDaughter('title', '', '', '', 'DAP Dashboard');
$head->addDaughter('meta', '', '', array('http-equiv' => 'refresh', 'content' => '60'));
$head->addDaughter('style', '', '', NULL, '
#container
{
    width: 1000px;
    height: 0px;
}
#container>div
{
    float: left;
    width: 300px;
    padding: 10px;
    border-right: 1px dashed;
}
table,th,td
{
    border: 1px solid black;
}
table
{
    border-collapse: collapse;
}
div.workspace
{
  font-size:200%;
}
');
$head->addDaughter('script', '', '', array('type' => 'text/javascript'), '
function deleteTask(task)
{
  document.getElementById(task + ".delete").value = "1";
  document.getElementById("control").submit();
}
');
$body = $html->addDaughter('body');
$container = $body->addDaughter('div', 'container', '');

$form = new HTMLNode('form', 'control', '', array('action' => 'index.php', 'method' => 'post'));

foreach($taskList as $task){
  $block = $container->addDaughter('div', $task, $task);

  $summary = file($task . '/summary.dat');
  $workspace = '';
  foreach($summary as $dataLine){
    $keyvalue = split(' = ', $dataLine);
    if($keyvalue[0] == 'workspace') $workspace = substr($keyvalue[1], strrpos($keyvalue[1], '/') + 1, strlen($keyvalue[1]));
  }

  $block->addDaughter('div', '', '', array('class' => 'workspace'), $workspace);

  $control = $block->addDaughter('div', '', '', NULL, ' ');

  $summaryTable = $block->addDaughter('table');

  $block->addDaughter('div', '', '', array('style' => 'margin-top:20px;'), new HTMLNode('a', '', '', array('href' => $task . '/logs/server.log'), 'Server log'));

  $table = $block->addDaughter('table', '', '', array('style' => 'margin-top:20px;'));
  $headerRow = $table->addDaughter('tr');
  $headerRow->addDaughter('th', '', '', NULL, 'Name');
  $headerRow->addDaughter('th', '', '', NULL, 'State');

  $stateSummary = array('UNKNOWN' => 0, 'CREATED' => 0, 'PENDING' => 0, 'RUNNING' => 0, 'DONE' => 0, 'FAILED' => 0, 'EXITED' => 0);
  $finished = TRUE;

  $stateList = array();
  foreach(scandir($task . '/status') as $stat){
    if($stat == '.' || $stat == '..') continue;
    $jobName = substr($stat, 0, strrpos($stat, '.'));
    $state = substr($stat, strrpos($stat, '.') + 1, strlen($stat));
    $stateList[$jobName] = $state;
  }

  ksort($stateList, SORT_NUMERIC);
  
  foreach($stateList as $jobName => $state){
    $stateSummary[$state] += 1;
    if($state == 'UNKNOWN' || $state == 'CREATED' || $state == 'PENDING' || $state == 'RUNNING') $finished = FALSE;

    $row = $table->addDaughter('tr');
    $nameCol = $row->addDaughter('td');
    $nameCol->addDaughter('a', '', '', array('href' => $task . '/logs/' . $jobName . '.log'), $jobName);
    $row->addDaughter('td', '', '', NULL, $state);
  }

  foreach($stateSummary as $key => $value){
    $row = $summaryTable->addDaughter('tr');
    $row->addDaughter('td', '', '', NULL, $key);
    $row->addDaughter('td', '', '', NULL, "$value");
  }

  if($finished)
    $control->addDaughter('input', '', '', array('value' => 'delete', 'type' => 'button', 'onClick' => 'deleteTask(\'' . $task . '\')'));

  $form->addDaughter('input', $task . '.delete', $task . '_delete', array('type' => 'hidden', 'value' => '0'));
}

$body->daughters[] = $form;

echo '<?xml version="1.0" encoding="utf-8" ?>' . "\n";
echo '<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">' . "\n";
echo $html->generateHTML();

?>