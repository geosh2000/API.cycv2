<?php
//defined('BASEPATH') OR exit('No direct script access allowed');
require( APPPATH.'/libraries/REST_Controller.php');
// use REST_Controller;


class Pya extends REST_Controller {

  public function __construct(){

    parent::__construct();
    $this->load->helper('json_utilities');
    $this->load->helper('validators');
    $this->load->helper('jwt');
    $this->load->database();
  }

  public function horarios_get(){

    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){
        
        $date = $this->uri->segment(3);
        $pais = $this->uri->segment(4);

        switch( $this->uri->segment(4) ){
          case 'MX':
            $pais = 1;
            break;
          case 'CO':
            $pais = 6;
            break;
          default:
            $pais = 6;
            break;
        }

        $this->db->select("a.*,
                            NOMBREASESOR(a.asesor, 1) as nombre,
                            NOMBREDEP(dep) as dep,
                            js, je, 
                            x1s, x1e, x2s, x2e,
                            cs, ce,
                            c.id as ausent_id, d.Ausentismo, e.Puesto as puesto, d.showPya, d.Code as Codigo, c.ausentismo as tipoAus, c.caso as casoAus, c.comments as notaAus, c.Last_Update as luAus, c.changed_by as chAus, NOMBREASESOR(c.changed_by,1) as nameChAus", FALSE)
            ->from("dep_asesores a")
            ->join("asesores_programacion b", "a.Fecha=b.Fecha AND a.asesor = b.asesor", "left")
            ->join("asesores_ausentismos c", "a.asesor = c.asesor AND a.Fecha = c.Fecha", "left")
            ->join("config_tiposAusentismos d", "c.ausentismo = d.id", "left", FALSE)
            ->join("PCRCs_puestos e", "a.puesto = e.id", "left")
            ->where("a.Fecha ", $date)
            ->where("VACANTE IS NOT ", "NULL", FALSE)
            ->where("dep !=", 29)
            ->where("dep !=", 1)
            ->where("dep !=", 47)
            ->where("a.operacion", $pais)
            ->order_by("dep, nombre");
            

      if( $q = $this->db->get() ){
          
        okResponse( 'Información de Horarios Obtenida', 'data', $q->result_array(), $this, 'rows', $q->num_rows() );

      }else{

        errResponse('Error al obtener data personal del asesor '.$asesor, REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());

      }


      return true;

    });

    jsonPrint( $result );

  }

  public function sesiones_get(){

    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){
        
        $date = $this->uri->segment(3);
        $pais = $this->uri->segment(4);

        switch( $this->uri->segment(4) ){
          case 'MX':
            $pais = 1;
            break;
          case 'CO':
            $pais = 6;
            break;
          default:
            $pais = 6;
            break;
        }

        $this->db->select("a.*")
            ->from("asesores_logs a")
            ->join("dep_asesores b", "a.asesor = b.asesor AND b.Fecha='$date'", "left", FALSE)
            
//            TOMA EN CUENTA 9 hrs ANTES DE LA FECHA ESTABLECIDA
            ->where("login >= ", "ADDTIME('$date 00:00:00', '-09:00:00')", FALSE)

//            SOLO LA FECHA ESTABLECIDA
//            ->where("login >= ", $date)

            ->where("login < ", "ADDDATE('$date',1)", FALSE)
            ->where("a.asesor !=", 0)
            ->where("b.operacion", $pais)
            ->where("dep !=", 29);
            

      if( $q = $this->db->get() ){

        $luQ = $this->db->query("SELECT MAX(logout) as lu FROM asesores_logs");
          
        okResponse( 'Información de Sesiones Obtenida', 'data', $q->result_array(), $this, 'lu', $luQ->row_array() );

      }else{

        errResponse('Error al obtener data personal del asesor '.$asesor, REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());

      }


      return true;

    });

    jsonPrint( $result );

  }
    
  public function exceptions_get(){

    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){
        
        $date = $this->uri->segment(3);
        $pais = $this->uri->segment(4);

        switch( $this->uri->segment(4) ){
          case 'MX':
            $pais = 1;
            break;
          case 'CO':
            $pais = 6;
            break;
          default:
            $pais = 6;
            break;
        }

        $this->db->select("a.*, Excepcion, Codigo, NOMBREASESOR(changed_by,1) as nombre")
            ->from("asesores_pya_exceptions a")
            ->join("config_tipos_pya_exceptions b", "a.tipo = b.id", "left")
            ->where("Fecha", $date);
            

      if( $qR = $this->db->get() ){
          
           $this->db->select("b.Code AS Codigo,
                            b.Ausentismo AS Excepcion,
                            Fecha,
                            Last_Update,
                            comments AS Nota,
                            asesor,
                            caso,
                            changed_by,
                            a.id,
                            NOMBREASESOR(changed_by, 1) AS nombre,
                            a.ausentismo AS tipo")
            ->from("asesores_ausentismos a")
            ->join("config_tiposAusentismos b", "a.ausentismo = b.id", "left")
            ->where("showPya", 1)
            ->where("Fecha", $date);

        if( $qA = $this->db->get() ){
          
           $result = $qA->result_array();
           $flag = true;
            
            foreach( $qR->result_array() as $index => $info ){
                foreach( $result as $resInd => $resInfo ){
                    if( $resInfo['asesor'] == $info['asesor'] ){
                        $flag = false;
                    }
                }
                
                if( $flag ){
                    array_push($result, $info);
                }
            }

        okResponse( 'Información de Excepciones Obtenida', 'data', $result, $this );

      }else{

        errResponse('Error al obtener data personal del asesor '.$asesor, REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());

      }

      }else{

        errResponse('Error al obtener data personal del asesor '.$asesor, REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());

      }


      return true;

    });

    jsonPrint( $result );

  }

 
}
