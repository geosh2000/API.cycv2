<?php
//defined('BASEPATH') OR exit('No direct script access allowed');
require( APPPATH.'/libraries/REST_Controller.php');
// use REST_Controller;


class Prenomina extends REST_Controller {

  public function __construct(){

    parent::__construct();
    $this->load->helper('json_utilities');
    $this->load->helper('jwt');
    $this->load->helper('validators');
    $this->load->database();

  }
    
    public function view_put(){

        $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

            $params = $this->put();
            $nomina = $params['corte'];
            $op     = $params['op'];
            
            $this->datesNomina( $params );
            $this->builtNomina( $params );
            $this->dateAsesorNomina( );
            
            if( $q = $this->db->query("SELECT * FROM dateAsesorNomina") ){

                okResponse( 'Dates Nomina Cargado', 'data', $q->result_array(), $this );

            }else{
                errResponse('Error en la base de datos', REST_Controller::HTTP_NOT_IMPLEMENTED, $this, 'error', $this->db->error());
            }

            return $result;

        });

        jsonPrint( $result );

    }
    
    public function schedules_put(){

        $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

            $params = $this->put();
            $nomina = $params['corte'];
            $op     = $params['op'];
            
            $this->datesNomina( $params );
            
            $this->db->select("a.Fecha, a.asesor,
                                a.dep, a.puesto, vacante,
                                hc_udn, hc_area, hc_dep, hc_puesto,  
                                js, je, x1s, x1e, x2s, x2e, phx")
                ->from("dep_asesores a")
                ->join("asesores_programacion b", "a.asesor = b.asesor AND a.Fecha = b.Fecha", 'left')
                ->join("Asesores c", "a.asesor = c.id", 'left')
                ->where("a.Fecha BETWEEN ", "@inicio AND @fin", FALSE)
                ->where("Egreso >= @fin","", FALSE)
                ->where("num_colaborador NOT LIKE '49500%'","", FALSE)
                ->where("a.operacion", $op)
                ->where("vacante IS NOT ", "NULL", FALSE)
                ->order_by("a.asesor, a.Fecha");
            
            if( $q = $this->db->get() ){

                okResponse( 'Dates Nomina Cargado', 'data', $q->result_array(), $this );

            }else{
                errResponse('Error en la base de datos', REST_Controller::HTTP_NOT_IMPLEMENTED, $this, 'error', $this->db->error());
            }

            return $result;

        });

        jsonPrint( $result );

    }  
    
    public function festivos_put(){

        $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

            $params = $this->put();
            $nomina = $params['corte'];
            $op     = $params['op'];
            
            $this->datesNomina( $params );
            
            $this->db->select("*")
                ->from("rrhh_festivos")
                ->where("Fecha BETWEEN ", "@inicio AND @fin", FALSE)
                ->order_by("Fecha");
            
            if( $q = $this->db->get() ){
                
                if($q->num_rows() > 0){
                    foreach($q->result_array() as $index => $f){
                        $result[$f['Fecha']]=$f['festivo'];
                    }
                }else{
                    $result = null;
                }

                okResponse( 'Dates Festivos Cargado', 'data', $result, $this );

            }else{
                errResponse('Error en la base de datos', REST_Controller::HTTP_NOT_IMPLEMENTED, $this, 'error', $this->db->error());
            }

            return $result;

        });

        jsonPrint( $result );

    }  
    
    public function asesores_put(){

        $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

            $params = $this->put();
            $nomina = $params['corte'];
            $op     = $params['op'];
            
            $this->datesNomina( $params );
            
            $this->builtNomina( $params );
            
            if( $q = $this->db->query("SELECT asesor, CLAVE, Nombre_del_empleado, Ubicacion, Unidad_de_Negocio, Area, Departamento, Puesto, NULL as Sueldo, NULL as Fac, Ingreso, Egreso as Baja, Salario FROM builtNomina") ){

                okResponse( 'Asesores Nomina Cargado', 'data', $q->result_array(), $this );

            }else{
                errResponse('Error en la base de datos', REST_Controller::HTTP_NOT_IMPLEMENTED, $this, 'error', $this->db->error());
            }

            return $result;

        });

        jsonPrint( $result );

    }  
    
    public function logs_put(){

        $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

            $params = $this->put();
            $nomina = $params['corte'];
            $op     = $params['op'];
            
            $this->datesNomina( $params );
            
            $query = "SELECT 
                                        a.Fecha, a.asesor, 
                                        checkLog(a.Fecha, a.asesor, 'in') AS login,
                                        checkLog(a.Fecha, a.asesor, 'out') AS logout
                                    FROM
                                        dep_asesores a
                                    WHERE
                                        a.Fecha BETWEEN @inicio AND @fin AND vacante IS NOT NULL 
                                        AND a.operacion = $op
                                    ORDER BY a.Fecha";
            
            if( $q = $this->db->query($query) ){

                $result=array();
  
                foreach($q->result_array() as $index => $info){
                    $tmp['login'] = $info['login'];
                    $tmp['logout'] = $info['logout'];
                    if( isset( $result[$info['Fecha']][$info['asesor']] ) ){
                        array_push($result[$info['Fecha']][$info['asesor']], $tmp);
                    }else{
                        $result[$info['Fecha']][$info['asesor']] = array($tmp);
                    }
                }
                okResponse( 'Logs Cargados', 'data', $result, $this );

            }else{
                errResponse('Error en la base de datos', REST_Controller::HTTP_NOT_IMPLEMENTED, $this, 'error', $this->db->error());
            }

            return $result;

        });

        jsonPrint( $result );

    }
    
    public function ausentismos_put(){

        $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

            $params = $this->put();
            $nomina = $params['corte'];
            $op     = $params['op'];
            
            $this->datesNomina( $params );
            
            if( $q = $this->db->query("SELECT 
                                        a.*, b.Ausentismo as a_name, Code
                                    FROM
                                        asesores_ausentismos a
                                            LEFT JOIN
                                        config_tiposAusentismos b ON a.ausentismo = b.id
                                    WHERE 
                                        Fecha BETWEEN @inicio AND @fin") ){

  
                foreach($q->result_array() as $index => $info){

                    $result[$info['Fecha']][$info['asesor']] = $info;
                    
                }
                
                okResponse( 'Ausentismos Cargados', 'data', $result, $this );

            }else{
                errResponse('Error en la base de datos', REST_Controller::HTTP_NOT_IMPLEMENTED, $this, 'error', $this->db->error());
            }

            return $result;

        });

        jsonPrint( $result );

    }   
    
    public function cxc_put(){

        $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

            $params = $this->put();
            $nomina = $params['corte'];
            $op     = $params['op'];
            
            $this->db->query("SELECT pago INTO @payday FROM rrhh_calendarioNomina WHERE id=$nomina");

            $this->db->query("DROP TEMPORARY TABLE IF EXISTS sumary");
            $this->db->query("CREATE TEMPORARY TABLE sumary SELECT 
                cxcId,
                montoFiscal,
                maxParcialidad,
                SUM(IF(IF(status = 1 AND CURDATE() >= ADDDATE(payday,-2),2,IF(status=0 AND CURDATE() >= ADDDATE(payday,-2),-1,status)) = 2, montoParcial, 0)) AS paidMonto,
                SUM(IF(IF(status = 1 AND CURDATE() >= ADDDATE(payday,-2),2,IF(status=0 AND CURDATE() >= ADDDATE(payday,-2),-1,status)) != 2, montoParcial, 0)) AS pendienteMonto,
                COUNT(*) AS parcialidades,
                COUNT(IF(IF(status = 1 AND CURDATE() >= ADDDATE(payday,-2),2,IF(status=0 AND CURDATE() >= ADDDATE(payday,-2),-1,status)) = 2, id, NULL)) AS paidParc
            FROM
                cxc_payTable
            WHERE 
                NOT (status=1 AND montoParcial = 0)
            GROUP BY cxcId");
            $this->db->query("ALTER TABLE sumary ADD PRIMARY KEY (cxcId)");

            $this->db->select("c.asesor,
                                SUM(IF(tipo=0, a.montoParcial, 0)) as monto_0,
                                GROUP_CONCAT(IF(tipo=0, CONCAT(Localizador,
                                            ' (',
                                            consecutivo,
                                            '/',
                                            parcialidades,
                                            ')'), NULL)
                                    SEPARATOR ', ') as locs_0,
                                SUM(IF(tipo=1, a.montoParcial, 0)) as monto_1,
                                GROUP_CONCAT(IF(tipo=1, CONCAT(Localizador,
                                            ' (',
                                            consecutivo,
                                            '/',
                                            parcialidades,
                                            ')'), NULL)
                                    SEPARATOR ', ') as locs_1", FALSE)
                    ->from('cxc_payTable a')
                    ->join('sumary b', 'a.cxcId = b.cxcId', 'left')
                    ->join('asesores_cxc c', 'a.cxcId = c.id', 'left')
                    ->where('payday = @payday', NULL, FALSE)
                    ->group_by('c.asesor');

            
            if( $q = $this->db->get() ){

                okResponse( 'CXC Cargados', 'data', $q->result_array(), $this );

            }else{
                errResponse('Error en la base de datos', REST_Controller::HTTP_NOT_IMPLEMENTED, $this, 'error', $this->db->error());
            }

            return $result;

        });

        jsonPrint( $result );

    }   
    
    public function listCortes_get(){

        $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

          if($q = $this->db->select("id, CONCAT(quincena, ' (', inicio, ' - ',fin,') | pago -> ',pago) as name")->order_by('inicio')->get_where('rrhh_calendarioNomina','inicio >= ADDDATE(CURDATE(),-125)')){
              
              $op = $this->db->get('hc_operacion');
              
              okResponse( 'Cortes Cargados', 'data', $q->result_array(), $this, 'ops', $op->result_array() );

            }else{
                errResponse('Error en la base de datos', REST_Controller::HTTP_NOT_IMPLEMENTED, $this, 'error', $this->db->error());
            }


          return $result;

        });

        jsonPrint( $result );

      }
    
    
    private function datesNomina( $params ){

        $nomina = $params['corte'];
        $op     = $params['op'];
        
        $this->db->query("DROP TEMPORARY TABLE IF EXISTS datesNomina");
        
        if( $this->db->query("CREATE TEMPORARY TABLE datesNomina SELECT
                            Fecha, inicio AS inicioNomina, fin AS finNomina
                        FROM
                            Fechas a
                                RIGHT JOIN
                            rrhh_calendarioNomina b ON a.Fecha BETWEEN inicio AND fin
                        WHERE
                            id = $nomina") ){
            $this->db->query("SET @inicio = (SELECT MIN(Fecha) FROM datesNomina)");
            $this->db->query("SET @fin = (SELECT MAX(Fecha) FROM datesNomina)");
            
            return true;

        }else{
            errResponse('Error en la base de datos', REST_Controller::HTTP_NOT_IMPLEMENTED, $this, 'error', $this->db->error());
        }
    }
    
    private function builtNomina( $params ){
        
        $nomina = $params['corte'];
        $op     = $params['op'];
        
        $this->db->query("DROP TEMPORARY TABLE IF EXISTS builtNomina");
        
        if( $this->db->query("CREATE TEMPORARY TABLE builtNomina SELECT
                          a.asesor,
                          num_colaborador AS CLAVE,
                          NOMBREASESOR(a.asesor,4) AS Nombre_del_empleado,
                          d.Ciudad AS Ubicacion,
                          NULL AS Centro_de_Costos,
                          g.nombre AS Unidad_de_Negocio,
                          f.nombre AS Area,
                          e.nombre AS Departamento,
                          h.nombre AS Puesto, 
                          c.esquema as Esquema,
                          Ingreso,
                          IF(Egreso>='2030-01-01',NULL,Egreso) as Egreso,
                          ROUND(SALARIOASESOR(a.asesor, @fin, 'salario'),
                                  2) AS Salario, a.operacion
                      FROM
                          dep_asesores a
                              LEFT JOIN
                          Asesores b ON a.asesor = b.id
                              LEFT JOIN
                          asesores_plazas c ON a.vacante = c.id
                              LEFT JOIN
                              cat_zones d ON c.ciudad = d.id
                              LEFT JOIN
                          hc_codigos_Departamento e ON a.hc_dep = e.id
                              LEFT JOIN
                          hc_codigos_Areas f ON e.area = f.id
                              LEFT JOIN
                          hc_codigos_UnidadDeNegocio g ON f.unidadDeNegocio = g.id
                              LEFT JOIN
                          hc_codigos_Puesto h ON a.hc_puesto = h.id
                      WHERE
                          Fecha = @fin AND vacante IS NOT NULL
                          AND Egreso >= @fin AND num_colaborador NOT LIKE '49500%' 
                          HAVING a.operacion=$op
                        ORDER BY num_colaborador") ){

            $this->db->query("ALTER TABLE builtNomina ADD PRIMARY KEY (asesor)");
            return true;

        }else{
            errResponse('Error en la base de datos', REST_Controller::HTTP_NOT_IMPLEMENTED, $this, 'error', $this->db->error());
        }
    }
    
    private function dateAsesorNomina( ){
        
        $this->db->query("DROP TEMPORARY TABLE IF EXISTS dateAsesorNomina");
        
        if( $this->db->query("CREATE TEMPORARY TABLE dateAsesorNomina SELECT
                        	Fecha, idAsesor
                        FROM
                        	datesNomina JOIN builtNomina") ){
            
            $this->db->query("ALTER TABLE dateAsesorNomina ADD PRIMARY KEY (Fecha, idAsesor)");
            return true;

        }else{
            errResponse('Error en la base de datos', REST_Controller::HTTP_NOT_IMPLEMENTED, $this, 'error', $this->db->error());
        }
    }
    
    
}