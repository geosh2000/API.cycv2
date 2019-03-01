<?php
//defined('BASEPATH') OR exit('No direct script access allowed');
require( APPPATH.'/libraries/REST_Controller.php');
// use REST_Controller;


class Survey extends REST_Controller {

  public function __construct(){

    parent::__construct();
    $this->load->helper('json_utilities');
    $this->load->helper('jwt');
    $this->load->helper('validators');
    $this->load->database();

  }

  public function survey_get(){

    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

      $id = $this->uri->segment(3);

      if( !$master = $this->db->select('*')
            ->from('form_Master')
            ->where('id', $id)
            ->get() ){
            errResponse('Error en DB', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
        }

      if( !$fields = $this->db->select('*')
            ->from('form_Fields')
            ->where('masterId', $id)
            ->order_by('order')
            ->get() ){
            errResponse('Error en DB', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
        }

        if( !$opt = $this->db->select('a.*')
            ->from('form_Opts a')
            ->join('form_Fields b', 'a.fieldId=b.id', 'left')
            ->where('masterId', $id)
            ->order_by('name')
            ->get() ){
            errResponse('Error en DB', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
        }

        if( !$graph = $this->db->select('*')
            ->from('form_sankeyParams a')
            ->where('masterId', $id)
            ->limit(1)
            ->get() ){
            errResponse('Error en DB', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
        }

        okResponse( 'Información de Survey Obtenida', 'data', array( 'master' => $master->row_array(), 'fields' => $fields->result_array(), 'opts' => $opt->result_array(), 'graph'=>$graph->row_array()), $this );

    });
  }

  public function save_put(){

    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

      $data = $this->put();

      $this->db->set($data['data']);

      if( $this->db->insert( $data['table'] ) ){
            okResponse( 'Formulario guardado correctamente', 'data', true, $this );
      }else{
            errResponse('Error al guardar formulario', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
      }


    });
  }

  public function summary_get(){

    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

        $asesor = $this->uri->segment(3);
        $master = $this->uri->segment(4);

        $mQ = $this->db->select('*')->from('form_Master')->where('id',$master)->get();
        $mR = $mQ->row_array();

        $fQ = $this->db->select('*')->from('form_Fields')->where('masterid',$master)->get();
        $fields = $fQ->result_array();

        $oQ = $this->db->select('*')->from('form_Opts')->get();
        $opts = array();
        foreach( $oQ->result_array() as $index => $info){
            $opts[$info['id']] = $info['name'];
        }


        $this->db->select('id, dtCreated')
                ->from($mR['targetTable'])
                ->where('master', $master)
                ->where('asesor', $asesor)
                ->order_by('dtCreated', 'desc')
                ->limit(10);

        foreach($fields as $index => $info){
            $this->db->select($info['targetField']." as ".$info['name']);
        }

        if( $r = $this->db->get() ){

            $result = $r->result_array();
            foreach( $result as $index => $data ){
                foreach($fields as $ind => $info){
                    if($info['type'] == 'select'){
                        if( $data[$info['name']] != null ){
                            $result[$index][$info['name']] = $opts[intVal($data[$info['name']])];
                        }
                    }
                }
            }
                okResponse( 'Historial Obtenido', 'data', $result, $this );
        }else{
                errResponse('Error al guardar formulario', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
        }

    });
  }

  public function results_get(){

    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

        $master = $this->uri->segment(3);
        $inicio = $this->uri->segment(4);
        $fin = $this->uri->segment(5);

        $mQ = $this->db->select('*')->from('form_Master')->where('id',$master)->get();
        $mR = $mQ->row_array();

        $fQ = $this->db->select('*')->from('form_Fields')->where('masterid',$master)->get();
        $fields = $fQ->result_array();

        $oQ = $this->db->select('*')->from('form_Opts')->get();
        $opts = array();
        foreach( $oQ->result_array() as $index => $info){
            $opts[$info['id']] = $info['name'];
        }


        $this->db->select('id, dtCreated, NOMBREASESOR(asesor,2) as NombreAsesor')
                ->from($mR['targetTable'])
                ->where('master', $master)
                ->where('dtCreated >=', $inicio)
                ->where('dtCreated <', "ADDDATE('$fin',1)", FALSE)
                ->order_by('dtCreated', 'desc');

        foreach($fields as $index => $info){
            $this->db->select($info['targetField']." as ".$info['name']);
        }

        if( $r = $this->db->get() ){

            $result = $r->result_array();
            foreach( $result as $index => $data ){
                foreach($fields as $ind => $info){
                    if($info['type'] == 'select'){
                        if( $data[$info['name']] != null ){
                            $result[$index][$info['name']] = $opts[intVal($data[$info['name']])];
                        }
                    }
                }
            }
                okResponse( 'Historial Obtenido', 'data', $result, $this );
        }else{
                errResponse('Error al guardar formulario', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
        }

    });
  }

  public function sankey_get(){

    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

        $master = $this->uri->segment(3);
        $inicio = $this->uri->segment(4);
        $fin = $this->uri->segment(5);

        $err = false;

        if( !$pQ = $this->db->query("SELECT * FROM form_sankeyParams WHERE masterId=$master ORDER BY step") ){
            $err = true;
        }

        if( $pQ->num_rows() <= 0 ){
            $err = true;
        }
        
        if( $err ){
            errResponse('Error al cargar parámetros del formulario', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', '');
        }

        $this->db->query("SET @inicio = '$inicio 00:00:00'");
        $this->db->query("SET @fin = '$fin 23:59:59'");
        $this->db->query("SET @master = $master");

        $this->db->query("DROP TEMPORARY TABLE IF EXISTS sankey");

        $query = "CREATE TEMPORARY TABLE sankey ";

        foreach( $pQ->result_array() as $index => $info ){
            if( $info['step'] != 1 ){
                $query .= " UNION ";
            }

            if( isset($info['isNull']) ){
                $isNullVal = $info['isNull']." ";
            }else{
                $isNullVal = "NULL";
            }

            $query .= "(SELECT 
                    COALESCE(CONCAT('".$info['prefixA']."',".$info['nameVal']."),".$isNullVal.") AS s_A, CONCAT('".$info['prefixB']."',".$info['nameVal_B'].") AS s_B, COUNT(*) AS q
                FROM
                    form_dataBase a ";
            
            if($info['join'] == '1'){
                $query .= " LEFT JOIN
                ".$info['joinTable']." ON ".$info['rightVal']." = ".$info['leftVal'];
            }

            if($info['join_B'] == '1'){
                $query .= " LEFT JOIN
                ".$info['joinTable_B']." ON ".$info['rightVal_B']." = ".$info['leftVal_B'];
            }
                        
            $query .= " WHERE
                    master = @master AND dtCreated BETWEEN @inicio AND @fin
                GROUP BY s_A , s_B
                HAVING s_A IS NOT NULL AND s_B IS NOT NULL)";
        }

        $this->db->query($query);

        $qF = "SELECT * FROM sankey";
        
        if( $r = $this->db->query($qF) ){
            // okResponse( 'Historial Obtenido', 'data', $r->result_array(), $this );
            
            $result = array();
            foreach( $r->result_array() as $index => $info ){
                array_push($result, array($info['s_A'],$info['s_B'],intVal($info['q'])));
            }
            okResponse( 'Historial Obtenido', 'data', $result, $this );

        }else{
                errResponse('Error al guardar formulario', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
        }

    });
  }

}