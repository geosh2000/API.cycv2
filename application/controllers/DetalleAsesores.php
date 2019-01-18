<?php
//defined('BASEPATH') OR exit('No direct script access allowed');
require( APPPATH.'/libraries/REST_Controller.php');
// use REST_Controller;

class DetalleAsesores extends REST_Controller {

  public function __construct(){

    parent::__construct();
    $this->load->helper('json_utilities');
    $this->load->helper('validators');
    $this->load->helper('jwt');
    $this->load->database();
    $this->load->model('AsesorGeneral_model');
    $this->load->model('AsesorVacantesHisto_model');
    $this->load->model('AsesorSolicitudesHisto_model');
  }

  public function detailAsesor_get(){

    $asesor_id = $this->uri->segment(3);

    // Validacion de id
    segmentSet( 3, 'Debe definir un id para buscar', $this );
    segmentType( 3, 'El id debe ser numérico', $this );

    $generales = $this->dataGeneral( $asesor_id );
    $movimientos = $this->dataMovimientos( $asesor_id );
    $solicitudes = $this->dataSolicitudes( $asesor_id );

    $dataGen = (array) $generales;
    $dataGen['solPendiente']      = $this->AsesorSolicitudesHisto_model->get_SolPendientes( $asesor_id );
    $dataGen['histo_puestos']     = (array) $movimientos;
    $dataGen['histo_solicitudes'] = (array) $solicitudes;

    okResponse("Registro cargado correctamente", "data", $dataGen, $this);

  }

  public function dataGeneral( $id, $allNULL = FALSE, $bypassVal = FALSE ){

    $asesor = $this->AsesorGeneral_model->get_asesor( $id, $allNULL );

    // Validator
    if( !$bypassVal  ){
      if( !isset($asesor) ){
        errResponse( "No existe asesor con id: $id", REST_Controller::HTTP_NOT_FOUND, $this );
        return;
      }
    }

    return $asesor;

  }

  public function dataMovimientos( $id, $allNULL = FALSE, $bypassVal = FALSE ){

    $movimientos = $this->AsesorVacantesHisto_model->get_movimientos( $id, $allNULL );

    // Validator
    if( !$bypassVal  ){
      if( !isset($movimientos) ){
        errResponse( "No existen movimientos para el asesor: $id", REST_Controller::HTTP_NOT_FOUND, $this );
        return;
      }
    }

    return $movimientos;

  }

  public function dataSolicitudes( $id, $allNULL = FALSE, $bypassVal = FALSE ){

    $solicitudes = $this->AsesorSolicitudesHisto_model->get_solicitudes( $id, $allNULL );

    // Validator
    if( !$bypassVal  ){
      if( !isset($solicitudes) ){
        errResponse( "No existen solicitudes para el asesor: $id", REST_Controller::HTTP_NOT_FOUND, $this );
        return;
      }
    }

    return $solicitudes;

  }

    
  public function detalle_get(){
      
      $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){
          
          $asesor = $this->uri->segment(3);
          
          if( $q = $this->db->query("SELECT 
                                a.id,
                                num_colaborador,
                                a.Nombre,
                                a.Nombre_Separado,
                                a.Apellidos_Separado,
                                `N Corto`,
                                Vigencia_Pasaporte,
                                Vigencia_Visa,
                                Telefono1,
                                Telefono2,
                                correo_personal,
                                CONCAT(username,'@pricetravel.com') as username,
                                d.id as profile_id,
                                RFC,
                                Fecha_Nacimiento,
                                profile_name,
                                IF(b.dep = 29,
                                    FINDSUPERDAYPDV(CURDATE(), b.oficina, 1),
                                    FINDSUPERDAYCC(CURDATE(), a.id, 1)) AS Supervisor,
                                CONCAT(hu.clave,'-',ha.clave,'-',hd.clave,'-',hp.clave) AS codigo,
                                CONCAT(hp.nombre, ' (', p.Puesto, ')') AS nombre_puesto,
                                pr.Departamento AS nombre_dep,
                                pdv.PDV
                            FROM
                                Asesores a
                                    LEFT JOIN
                                dep_asesores b ON a.id = b.asesor AND b.Fecha = CURDATE()
                                    LEFT JOIN
                                userDB c ON a.id = c.asesor_id
                                    LEFT JOIN
                                profilesDB d ON c.profile = d.id
                                    LEFT JOIN
                                hc_codigos_UnidadDeNegocio hu ON b.hc_udn = hu.id
                                    LEFT JOIN
                                hc_codigos_Areas ha ON b.hc_area = ha.id
                                    LEFT JOIN
                                hc_codigos_Departamento hd ON b.hc_dep = hd.id
                                    LEFT JOIN
                                hc_codigos_Puesto hp ON b.hc_puesto = hp.id
                                    LEFT JOIN
                                PCRCs_puestos p ON b.puesto = p.id
                                    LEFT JOIN
                                PCRCs pr ON b.dep = pr.id
                                    LEFT JOIN
                                PDVs pdv ON b.oficina=pdv.id
                            WHERE
                                a.id = $asesor")){
              
              okResponse("Registro cargado correctamente", "data", $q->row_array(), $this);
          }else{
              errResponse('Error al compilar información', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
          }
             
      });


  }
    
  public function contrato_get(){
      
      $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){
          
          $asesor = $this->uri->segment(3);
          
          $this->db->query("SET @asesor=$asesor");

          $query = "SELECT 
                        NOMBREASESOR(a.id,2) as nombre, 
                        Ingreso,
                        Egreso, 
                        TIMESTAMPDIFF(YEAR, Ingreso, IF(Egreso<'2030-01-01',Egreso,CURDATE())) as old_years,
                        TIMESTAMPDIFF(MONTH, Ingreso, IF(Egreso<'2030-01-01',Egreso,CURDATE())) as old_months,
                        TIMESTAMPDIFF(DAY, Ingreso, IF(Egreso<'2030-01-01',Egreso,CURDATE())) as old_days,
                        IF(Egreso<CURDATE(),0,1) AS activo, c.recontratable, 
                        d.tipo, inicio, fin, IF(fin IS NOT NULL AND fin<CURDATE() AND Egreso>CURDATE(), 1, 0) as vencido,
                        IF(e.id IS NOT NULL,1,0) as activeSol,
                        a.id as idAsesor,
                        ev.id as evalId,
                        d.id as contratoId
                    FROM
                        Asesores a
                            LEFT JOIN
                        userDB b ON a.id = b.asesor_id
                            LEFT JOIN
                        asesores_recontratable c ON a.id = c.asesor
                            LEFT JOIN
                        asesores_contratos d ON a.id=d.asesor AND d.activo=1
                            LEFT JOIN 
                        asesores_evaluacionD ev ON d.id=ev.id
                            LEFT JOIN
                        rrhh_solicitudesCambioBaja e ON a.id=e.asesor AND e.status=0
                    WHERE
                        a.id = @asesor AND COALESCE(deleted,0)=0";
          
          $contratos = "SELECT 
                               a.*,
                                ev.id as evalId, ev.status, NOMBREASESOR(a.asesor,2) as Nombre
                          FROM
                            asesores_contratos a LEFT JOIN asesores_evaluacionD ev ON a.id=ev.contrato
                          WHERE
                            a.asesor = @asesor AND COALESCE(deleted,0)=0";
          
          if( $q = $this->db->query($query)){
              
              if( $c = $this->db->query($contratos)){
                  
                  $s = $this->db->query("SELECT 
                                        *
                                    FROM
                                        rrhh_solicitudesCambioBaja a
                                    WHERE
                                        asesor = @asesor AND a.status = 0");
                  
                  okResponse("Registro cargado correctamente", "data", array('actual' => $q->row_array(), 'contratos' => $c->result_array()), $this, 'pendientes', $s->num_rows());
              }else{
                  errResponse('Error al compilar información de contratos', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
              }

          }else{
              errResponse('Error al compilar información', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
          }
             
      });


  }
    
  public function historial_get(){
      
      $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){
          
          $asesor = $this->uri->segment(3);
          
          $this->db->query("SET @asesor=$asesor");

          $query = "(SELECT 
                        fecha_in AS Fecha,
                        vacante,
                        a.id AS move_id,
                        c.Ciudad,
                        d.PDV,
                        pr.Departamento,
                        pu.Puesto,
                        IF(CURDATE() > Egreso, 0, 1) AS activo
                    FROM
                        asesores_movimiento_vacantes a
                            LEFT JOIN
                        asesores_plazas b ON a.vacante = b.id
                            LEFT JOIN
                        cat_zones c ON b.ciudad = c.id
                            LEFT JOIN
                        PDVs d ON b.oficina = d.id
                            LEFT JOIN
                        PCRCs pr ON b.departamento = pr.id
                            LEFT JOIN
                        PCRCs_puestos pu ON b.puesto = pu.id
                            LEFT JOIN
                        Asesores x ON a.asesor_in = x.id
                    WHERE
                        asesor_in = @asesor
                    ORDER BY fecha_in DESC) UNION
                    (SELECT 
                        fecha AS Fecha,
                        NULL AS vacante,
                        movimientoID AS move_id,
                        NULL AS Ciudad,
                        NULL AS PDV,
                        'Baja' AS Departamento,
                        NULL AS Puesto,
                        0 AS activo
                    FROM
                        rrhh_solicitudesCambioBaja a
                    WHERE
                        asesor = @asesor AND tipo = 2
                            AND a.status = 1) ORDER BY Fecha DESC";

          
          if( $q = $this->db->query($query)){
              okResponse("Registro cargado correctamente", "data", $q->result_array(), $this);
          }else{
              errResponse('Error al compilar información', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
          }
             
      });


  }  
    
  public function vacaciones_get(){
      
      $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){
          
          $asesor = $this->uri->segment(3);
          
          $this->db->query("SET @asesor=$asesor");

          $query = "SELECT 
                        *
                    FROM
                        asesores_vacaciones
                    WHERE
                        asesor = $asesor
                    ORDER BY Fecha ";

          
          if( $q = $this->db->query($query)){

            $queryDet = "SELECT 
                            id,
                            MIN(Fecha) AS Inicio,
                            MAX(Fecha) AS Fin,
                            SUM(a) AS Dias,
                            caso AS Caso,
                            Last_Update AS FechaCaptura,
                            NOMBREASESOR(changed_by,1) as Captura
                        FROM
                            asesores_ausentismos
                        WHERE
                            asesor = $asesor AND ausentismo = 1
                        GROUP BY id
                        ORDER BY Inicio";


            if( $d = $this->db->query($queryDet)){
                okResponse("Registro cargado correctamente", "data", $q->result_array(), $this, 'detalle', $d->result_array());
            }else{
                errResponse('Error al compilar información', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
            }

          }else{
              errResponse('Error al compilar información', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
          }
             
      });


  }  
  
  public function solicitudes_get(){
      
      $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){
          
          $asesor = $this->uri->segment(3);
          
          $this->db->query("SET @asesor=$asesor");

          $query = "SELECT 
                        a.fecha_solicitud AS Fecha_Solicitud,
                        a.fecha AS Fecha_Cambio,
                        vacante AS vacante_solicitada,
                        a.id AS id_sol,
                        a.tipo AS tipo,
                        a.movimientoID AS move_id,
                        a.reemplazable,
                        a.recontratable,
                        solicitado_por as solicitante_id,
                        NOMBREASESOR(solicitado_por, 1) AS solicitante,
                        NOMBREASESOR(aprobado_por, 1) AS aprobante,
                        fecha_aprobacion AS Fecha_Aprobacion,
                        a.comentarios AS com_solicitante,
                        comentariosRRHH AS com_aprobante,
                        a.status AS status,
                        c.Ciudad,
                        d.PDV,
                        pr.Departamento,
                        pu.Puesto,
                        IF(CURDATE() > Egreso, 1, 0) AS activo
                    FROM
                        rrhh_solicitudesCambioBaja a
                            LEFT JOIN
                        asesores_plazas b ON a.vacante = b.id
                            LEFT JOIN
                        cat_zones c ON b.ciudad = c.id
                            LEFT JOIN
                        PDVs d ON b.oficina = d.id
                            LEFT JOIN
                        PCRCs pr ON b.departamento = pr.id
                            LEFT JOIN
                        PCRCs_puestos pu ON b.puesto = pu.id
                            LEFT JOIN
                        Asesores x ON a.asesor = x.id
                    WHERE
                        a.asesor = @asesor
                    ORDER BY fecha_solicitud DESC";

          
          if( $q = $this->db->query($query)){
              okResponse("Registro cargado correctamente", "data", $q->result_array(), $this);
          }else{
              errResponse('Error al compilar información', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
          }
             
      });


  }

}
