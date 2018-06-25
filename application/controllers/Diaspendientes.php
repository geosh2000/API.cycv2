<?php
//defined('BASEPATH') OR exit('No direct script access allowed');
require( APPPATH.'/libraries/REST_Controller.php');
// use REST_Controller;


class Diaspendientes extends REST_Controller {

  public function __construct(){

    parent::__construct();
    $this->load->helper('json_utilities');
    $this->load->helper('validators');
    $this->load->helper('jwt');
    $this->load->database();
  }

  public function getSummary_get(){

    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){
        
        $asesor=$this->uri->segment(3);
        
//        COMPILE DPEND
        $this->db->select("asesor,
                            SUM(IF(phx = 0, phx_done, 0)) * 2 / 8 AS done,
                            SUM(phx_paid) AS paid,
                            SUM(IF(phx = 0, phx_done, 0)) AS hx,
                            0 AS dt,
                            0 AS special")
            ->from("asesores_programacion", FALSE)
            ->group_by("asesor");
        
        if( isset($asesor) ){
            $this->db->where('asesor', $asesor);
        }
        
        $dPend = $this->db->get_compiled_select();
        
        $this->db->query("DROP TEMPORARY TABLE IF EXISTS dPend");
        $this->db->query("CREATE TEMPORARY TABLE dPend $dPend");
        $this->db->query("ALTER TABLE dPend ADD PRIMARY KEY (asesor)");
        
        
//        COMPILE INSERT DTS
        $this->db->select("asesor,
                            SUM(IF(pdt = 0, IF(pdt_done % floor(pdt_done) >= 0.78, ROUND(pdt_done), pdt_done), 0)) * 2 / 8 AS dtdone,
                            SUM(pdt_paid) AS dtpaid,
                            0 AS hx,
                            SUM(IF(pdt = 0, IF(pdt_done % floor(pdt_done) >= 0.78, ROUND(pdt_done), pdt_done), 0)) AS dt,
                            0 AS special", FALSE)
            ->from("asesores_ausentismos")
            ->group_by("asesor");
        
        if( isset($asesor) ){
            $this->db->where('asesor', $asesor);
        }
        
        $insDT = $this->db->get_compiled_select();

        $this->db->query("INSERT INTO dPend 
            SELECT * FROM ($insDT) a
        ON DUPLICATE KEY UPDATE done=done+dtdone, paid=paid+dtpaid, dt=a.dt");
        
        //        COMPILE INSERT SPECIALS
        $this->db->select("asesor,
                            SUM(horas) / 8 AS dtdone,
                            0 AS dtpaid,
                            0 AS hx,
                            0 AS dt,
                            SUM(horas) AS especiales", FALSE)
            ->from("asesores_diasPendientes")
            ->where("status", 1)
            ->group_by("asesor");
        
        if( isset($asesor) ){
            $this->db->where('asesor', $asesor);
        }
        
        $insSpe = $this->db->get_compiled_select();

        $this->db->query("INSERT INTO dPend 
            SELECT * FROM ($insSpe) a 
        ON DUPLICATE KEY UPDATE done=done+dtdone, special=special+especiales");


        $query = "SELECT 
            NOMBREASESOR(b.asesor, 2) AS Nombre, a.asesor, dep, 
            COALESCE(hx,0) as horas_extra_x2,
            COALESCE(dt,0) as horas_dts_x2,
            COALESCE(special,0) as horas_especial_x1,
            COALESCE(done,0) as dias_totales,
            COALESCE(paid,0) as dias_pagados,
            FLOOR(COALESCE(done,0)-COALESCE(paid,0)) as disponibles
        FROM
            dep_asesores a LEFT JOIN
            dPend b ON a.asesor=b.asesor
            WHERE Fecha=CURDATE() AND vacante IS NOT NULL HAVING Nombre IS NOT NULL";
        
        if( !isset($asesor) ){
            $query .= " AND disponibles != 0";
        }
        
      if( $q = $this->db->query($query) ){
          
        okResponse( 'Información Obtenida', 'data', isset($asesor) ? $q->row_array() : $q->result_array(), $this );

      }else{

        errResponse('Error al obtener slots en tiempo real', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());

      }


      return true;

    });

    jsonPrint( $result );

  }
    
  public function saveAdd_put(){

    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){
        
        $data = $this->put();
        $extra = array( 'created_by' => $_GET['usid']);
            
        $this->db->set($data)
            ->set($extra);
        

      if( $this->db->insert('asesores_diasPendientes') ){
          
        okResponse( 'Información Guardada', 'data', true, $this );

      }else{

        errResponse('Error al guardar registro', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());

      }


      return true;

    });

    jsonPrint( $result );

  }
    
  public function toApprobe_get(){

    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){
        
        $this->db->select('NOMBREASESOR(asesor,2) as Nombre, asesor, Fecha, horas, motivo, NOMBREASESOR(created_by,1) as creador', FALSE)
            ->from('asesores_diasPendientes')
            ->where('status', 0)
            ->order_by('Nombre');
        

      if( $q = $this->db->get() ){
          
        okResponse( 'Información Obtenida', 'data', $q->result_array(), $this );

      }else{

        errResponse('Error al obtener información', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());

      }


      return true;

    });

    jsonPrint( $result );

  }  
    
  public function rejected_get(){

    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){
        
        $this->db->select('NOMBREASESOR(asesor,2) as Nombre, asesor, Fecha, horas, motivo, NOMBREASESOR(created_by,1) as creador, NOMBREASESOR(approber,1) as creador, date_approbe', FALSE)
            ->from('asesores_diasPendientes')
            ->where('status', 2)
            ->order_by('Nombre');
        

      if( $q = $this->db->get() ){
          
        okResponse( 'Información Obtenida', 'data', $q->result_array(), $this );

      }else{

        errResponse('Error al obtener información', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());

      }


      return true;

    });

    jsonPrint( $result );

  }
    
  public function approbe_put(){

    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){
        
        $data = $this->put();
        $extra = array( 'approber' => $_GET['usid'], 'status' => $data['status']);
            
        $this->db->where($data['item'])
            ->set($extra)
            ->set('date_approbe', 'NOW()', FALSE);
        

      if( $this->db->update('asesores_diasPendientes') ){
          
        okResponse( 'Información Guardada', 'data', true, $this );

      }else{

        errResponse('Error al guardar registro', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());

      }


      return true;

    });

    jsonPrint( $result );

  }
    
  public function detail_get(){

    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){
        
        $asesor = $this->uri->segment(3);
        
        $this->db->query("SET @asesor = $asesor");
        
        $sp = "SELECT 
            Fecha,
            motivo,
            horas,
            a.status,
            NOMBREASESOR(created_by, 1) AS captura,
            date_created as fecha_captura,
            NOMBREASESOR(approber, 1) AS aprueba,
            date_approbe as fecha_aprobacion, 
            Last_Update as Ultima_modificacion
        FROM
            asesores_diasPendientes a
        WHERE
            asesor = @asesor
        ORDER BY Fecha DESC";
        
        $hx = "SELECT 
            id, Fecha, x1s, x1e, x2s, x2e, phx_done
        FROM
            asesores_programacion
        WHERE
            asesor = @asesor
                AND (x1s != x1e OR x2s != x2e)
                AND phx = 0
            ORDER BY Fecha DESC";
        
        
        $dt = "SELECT 
            id,
            Fecha,
            comments,
            pdt_done,
            NOMBREASESOR(changed_by, 1) AS captura
        FROM
            asesores_ausentismos
        WHERE
            pdt = 0 AND ausentismo = 19
                AND asesor = @asesor
            ORDER BY Fecha DESC";
        
        $paid = "SELECT 
                id,
                CONCAT(MIN(Fecha),' al ',MAX(Fecha)) as Fechas,
                SUM(pdt_paid) as dias_pagados,
                caso,
                comments,
                NOMBREASESOR(changed_by, 1) AS capturado, Last_Update as Fecha_Captura
            FROM
                asesores_ausentismos
            WHERE
                pdt_paid > 0 AND
                asesor=@asesor
            GROUP BY id
            ORDER BY Fecha DESC";
        

      if( !$qhx     = $this->db->query($hx) )   { $this->err($this->db->error()); }
      if( !$qdt     = $this->db->query($dt) )   { $this->err($this->db->error()); }
      if( !$qsp     = $this->db->query($sp) )   { $this->err($this->db->error()); }
      if( !$qpaid   = $this->db->query($paid) ) { $this->err($this->db->error()); }
          
      okResponse( 'Información Obtenida', 'data', array( 'hx' => $qhx->result_array(), 'dt' => $qdt->result_array(), 'sp' => $qsp->result_array(), 'paid' => $qpaid->result_array()), $this );


      return true;

    });

    jsonPrint( $result );

  } 

    
  private function err($error){
        errResponse('Error al obtener información', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $err);
  }
    
   

 
}
