<?php
//defined('BASEPATH') OR exit('No direct script access allowed');
require( APPPATH.'/libraries/REST_Controller.php');
// use REST_Controller;


class VentaMonitor extends REST_Controller {

    public function __construct(){

        parent::__construct();
        $this->load->helper('json_utilities');
        $this->load->helper('validators');
        $this->load->helper('jwt');
        $this->load->database();
    }

    public function getRN_post(){

        $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

        $data = $this->post();

        $this->db->query("SET @inicio = CAST('".$data['start']."' as DATE)");
        $this->db->query("SET @fin    = CAST('".$data['end']."' as DATE)");
        $this->db->query("SET @pais   = '".$data['pais']."'");
        $this->db->query("SET @marca  = '".$data['marca']."'");

        $this->db->query("DROP TEMPORARY TABLE IF EXISTS hotelesRAW");

        $this->db->select("a.*", FALSE)
                ->select("if(VentaMXN>0,CONCAT(Localizador,"-",item),null) as NewLoc", FALSE)
                ->select("IF(tipoCanal = 'Movil', 'Online', tipoCanal) as tipoCanal", FALSE)
                ->select("gpoCanalKpi")
                ->from("t_hoteles_test a")
                ->join("chanGroups b", "a.chanId = b.id", "left")
                ->where("Fecha BETWEEN ", "@inicio AND IF(@fin>CURDATE(),CURDATE(),@fin)", FALSE)
                ->where(array( 'categoryId' => 1, 'pais' => $data['pais'], 'marca' => $data['marca'] ));

        $hotelesRAW = $this->db->get_compiled_select();

        if( $this->db->query("CREATE TEMPORARY TABLE hotelesRAW $hotelesRAW") ){

            $this->db->query("ALTER TABLE hotelesRAW ADD PRIMARY KEY (`Localizador`, `Fecha`, `Hora`, `item`)");
            $this->db->query("SELECT @maxDate := MAX(Fecha) FROM hotelesRAW");

            $this->db->select("a.*", FALSE)
                    ->select("if(VentaMXN>0,CONCAT(Localizador,"-",item),null) as NewLoc", FALSE)
                    ->select("IF(tipoCanal = 'Movil', 'Online', tipoCanal) as tipoCanal", FALSE)
                    ->select("gpoCanalKpi")
                    ->from("t_hoteles_test a")
                    ->join("chanGroups b", "a.chanId = b.id", "left")
                    ->where("Fecha BETWEEN ", "IF(@maxDate IS NULL, @inicio, @maxDate) AND IF(@fin>CURDATE(),CURDATE(),@fin)", FALSE)
                    ->where(array( 'categoryId' => 1, 'pais' => $data['pais'], 'marca' => $data['marca'] ));

            $hotelesRAW = $this->db->get_compiled_select();

            if( $this->db->query("INSERT INTO hotelesRAW (SELECT * FROM ($hotelesRAW) a) ON DUPLICATE KEY UPDATE VentaMXN = a.VentaMXN") ){

            $this->db->select("Fecha, gpoCanalKpi, tipoCanal, SUM(clientNights) as RN_w_xld, SUM(IF(clientNights>0,clientNights,0)) as RN", FALSE)
                    ->from('hotelesRAW')
                    ->group_by("Fecha, gpoCanalKpi, tipoCanal");

            if( $dates = $this->db->get() ){

                $this->db->select("gpoCanalKpi, tipoCanal, SUM(clientNights) as RN_w_xld, SUM(IF(clientNights>0,clientNights,0)) as RN", FALSE)
                        ->from('hotelesRAW')
                        ->group_by("gpoCanalKpi, tipoCanal");

                if( $all = $this->db->get() ){

                $luq = $this->db->query("SELECT MAX(Last_Update) as LU FROM t_hoteles_test WHERE Fecha=CURDATE()");

                $data = array( 'dates' => $dates->result_array(), 'all' => $all->result_array(), 'lu' => $luq->row_array());

                okResponse( 'Data obtenida', 'data', $data, $this, 'lu', $lu );

                }else{
                errResponse('Error al compilar data por Rango', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
                }

            }else{
                errResponse('Error al compilar data por Fecha', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
            }


            }else{
            errResponse('Error al insertar data actual a hotelesRAW', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
            }

        }else{
            errResponse('Error al compilar información hotelesRAW', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
        }



            return true;

        });

        jsonPrint( $result );

    }

    public function getVentaAsesorGraph_get(){

        $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

        $asesor = $this->uri->segment(3);
        segmentSet(   3, 'Debes ingresar un asesor', $this );
        segmentType(  3, "El input debe ser de tipo entero -> $asesor", $this );

        $this->db->query("SET @inicio = CAST(CONCAT(YEAR(CURDATE()), '-', MONTH(CURDATE()), '-01') AS DATE);");
        if( $q = $this->db->query("SELECT 
                                        a.asesor,
                                        a.Fecha AS fecha,
                                        a.Last_Update AS Last_update,
                                        IF(js IS NULL OR js = je, 0,1) as ausentismo,
                                        callsIn AS llamadas,
                                        MontoInAll AS monto,
                                        MontoNotInAll AS monto_else,
                                        locsIn AS rsvas,
                                        locsNotIn AS rsvas_else
                                    FROM
                                        graf_dailySale a LEFT JOIN asesores_programacion b ON a.asesor=b.asesor AND a.Fecha=b.Fecha
                                    WHERE
                                        a.asesor = $asesor
                                            AND a.Fecha BETWEEN @inicio AND CURDATE()
                                    ORDER BY a.Fecha") ){
            okResponse( "Información correctamente obtenida", 'data', $q->result_array(), $this );
        }else{
            errResponse('Error en la base de datos', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
        }


        return true;

        });

        jsonPrint( $result );

    }
    
    public function ivrPart_put(){

        $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){
            
            $data = $this->put();

            $fecha = $data['date'];
            $perHour = intVal($data['porCola']);
            $soporte = intVal($data['soporte']);
            
            // okResponse( "Información correctamente obtenida", 'data', $soporte, $this );
            
            $fechaEnd = $fecha;
                
            $this->buildCalls( $fecha, $fechaEnd, 1, $soporte );

            $a = "SELECT Origin, Path as destination, COUNT(*) as calls FROM callsDep GROUP BY Origin, Path ORDER BY destination, Origin";
            $b = "SELECT Path as Origin, Grupo as destination, COUNT(*) as calls FROM callsDep GROUP BY Path, Grupo ORDER BY destination";
            $c = "SELECT Grupo as Origin, HOUR(Hora) as destination, COUNT(*) as calls FROM callsDep GROUP BY Grupo, destination ORDER BY destination";
            $d = "SELECT HOUR(Hora) as Origin, CONCAT('_',Path) as destination, COUNT(*) as calls FROM callsDep GROUP BY HOUR(Hora), destination ORDER BY destination";
            
            if( $qA = $this->db->query($a) ){
            
                    if( $qB = $this->db->query($b) ){
                        
                    if( $qC = $this->db->query($c) ){
                        
                            $arrA = $qA->result_array();
                            $arrB = $qB->result_array();
                            $arrC = $qC->result_array();

                            $result = array();

                            foreach($arrA as $index => $info){
                                array_push($result, array(str_replace(" ","<br>",$info['Origin']), str_replace(" ","<br>",$info['destination']), intval($info['calls'])));
                            }

                            foreach($arrB as $index => $info){
                                array_push($result, array(str_replace(" ","<br>",$info['Origin']), str_replace(" ","<br>",$info['destination']), intval($info['calls'])));
                            }
                        
                            foreach($arrC as $index => $info){
                                array_push($result, array(str_replace(" ","<br>",$info['Origin']), intval($info['destination']), intval($info['calls'])));
                            }
                        
                        if( $perHour == 1 ){
                                if( $qD = $this->db->query($d) ){
                                    $arrD = $qD->result_array();
                                    
                                    foreach($arrD as $index => $info){
                                        array_push($result, array(intval($info['Origin']), $info['destination'], intval($info['calls'])));
                                    }
                                }
                        }
                        
                        $lu = $this->db->query("SELECT MAX(Hora) as lu FROM calls");
                        $luT = $lu->row_array();
                        
                        $total = $this->db->query("SELECT COUNT(*) as ofrecidas, COUNT(IF(Grupo != 'Abandon', Grupo, NULL)) as contestadas, COUNT(IF(Grupo = 'Abandon', Grupo, NULL)) as abandonadas FROM callsDep");
                        
                        if( $soporte == 1 ){
                            $dids = $this->db->query("SELECT Fecha, Hora, Llamante, Origin, DNIS, Llamante FROM callsDep WHERE Desconexion != 'Abandono' ORDER BY Origin, Fecha, Hora");
                            $result = array( 'result' => $result, 'total' => $total->row_array(), 'dids' => $dids->result_array() );
                        }else{
                            $result = array( 'result' => $result, 'total' => $total->row_array() );
                        }

                            okResponse( "Información correctamente obtenida", 'data', $result, $this, 'lu', $fecha." ".$luT['lu'] );
                        
                        }else{
                            errResponse('Error en la base de datos', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
                        }

                    }else{
                        errResponse('Error en la base de datos', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
                    }
                
        }else{
            errResponse('Error en la base de datos', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
        }


            return true;

        });

        jsonPrint( $result );

    }
    
    public function callsKpis_put(){

        $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){
            
            $data = $this->put();

            $fecha = $data['Fecha'];
            $hora = $data['Hora'];
            
            if( $data['h'] == 1 ){
                $this->buildCalls( $fecha, $fecha, 1, 0, $data['skill'] );
            }else{
                $this->buildCalls( $fecha, $fecha, 1, 0, $data['skill'], $hora);
            }


            $this->db->select("COUNT(IF(direction = 1 AND Desconexion != 'Abandon',Llamante,NULL)) as Ans, COUNT(IF(direction = 1 AND Desconexion = 'Abandon',Llamante,NULL)) as Abn", FALSE)
                    ->from("callsDep");
            
            if( $a = $this->db->get() ){

                okResponse( "Información correctamente obtenida", 'data', $a->result_array(), $this );

            }else{
                errResponse('Error en la base de datos', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
            }


        return true;

        });

        jsonPrint( $result );

    }
    
    public function callStats_put(){

        $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){
            
            $data = $this->put();

            $fecha = $data['date'];
            $fin = $data['end'];

            $this->buildCalls( $fecha, $fin, 1, 0, $data['skill']);

            $this->db->select("Grupo, COUNT(*) as calls, AVG(COALESCE(TIME_TO_SEC(Duracion),0)) as AHT", FALSE)
                    ->from("callsDep")
                    ->group_by(array('H','Grupo'))
                    ->order_by("Grupo, Fecha, Hora");

            switch( $data['groupBy'] ){
                case 'hora':
                    $this->db->select("CAST(CONCAT(Fecha,' ',CONCAT(HOUR(Hora),IF(MINUTE(Hora)>=30,':30:00',':00:00'))) as DATETIME) as H", FALSE);
                    break;
                case 'dia':
                    $this->db->select("CAST(CONCAT(Fecha,' 00:00:00') as DATETIME) as H", FALSE);
                    break;
                case 'mes':
                    $this->db->select("CAST(CONCAT(YEAR(Fecha),'-',MONTH(Fecha),'-01 00:00:00') as DATETIME) as H", FALSE);
                    break;
                case 'inDay':
                    $this->db->select("CAST(CONCAT('$fin ',CONCAT(HOUR(Hora),IF(MINUTE(Hora)>=30,':30:00',':00:00'))) as DATETIME) as H", FALSE);
                    break;
            }
            
            if( $a = $this->db->get() ){
                
                $this->db->query("CREATE TEMPORARY TABLE resultCallsFC SELECT 
                                            a.Fecha as FechaOK,
                                            CAST(CONCAT(FLOOR(hora / 2),
                                                        ':',
                                                        IF(hora MOD 2 = 0, '00', '30'),
                                                        ':00')
                                                AS TIME) as horaOK,
                                            ROUND(volumen * participacion,0) AS calls
                                        FROM
                                            forecast_volume a
                                                LEFT JOIN
                                            forecast_participacion b ON a.Fecha = b.Fecha AND a.skill = b.skill
                                        WHERE
                                            a.Fecha BETWEEN '$fecha' AND '$fin' AND a.skill = ".$data['skill']."
                                        ORDER BY FechaOK");

                $this->db->from('resultCallsFC');

                switch( $data['groupBy'] ){
                    case 'hora':
                        $this->db->select("FechaOK as Fecha, CAST(CONCAT(FechaOK,' ',horaOK) as DATETIME) as hora, CONCAT(FechaOK,' ',horaOK) as test", FALSE)
                        ->select("SUM(calls) as calls")
                        ->group_by(array('Fecha','hora'));
                        break;
                    case 'dia':
                        $this->db->select("FechaOK as Fecha, CAST(CONCAT(FechaOK,' 00:00:00') as DATETIME) as hora", FALSE)
                        ->select("SUM(calls) as calls")
                        ->group_by(array('Fecha','hora'));
                        break;
                    case 'mes':
                        $this->db->select("FechaOK as Fecha, CAST(CONCAT(YEAR(FechaOK),'-',MONTH(FechaOK),'-01 00:00:00') as DATETIME) as hora", FALSE)
                        ->select("SUM(calls) as calls")
                        ->group_by(array('Fecha','hora'));
                        break;
                    case 'inDay':
                        $this->db->select("'$fin' as Fecha, CAST(CONCAT('$fin ',horaOK) as DATETIME) as hora", FALSE)
                        ->group_by(array('hora'));
                        break;
                }
                
                $fc = $this->db->get();

                okResponse( "Información correctamente obtenida", 'data', $a->result_array(), $this, 'forecast', $fc->result_array() );

            }else{
                errResponse('Error en la base de datos', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
            }


        return true;

        });

        jsonPrint( $result );

    }
    
    public function callStatsH_put(){

        $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){
            
            $data = $this->put();

            $inicio = $data['date'];
            $fin = $data['dateEnd'];

            $this->buildCalls( $inicio, $fin, 1, 0, $data['skill'] );

            $this->db->select("Grupo, COUNT(*) as calls, AVG(COALESCE(TIME_TO_SEC(Duracion),0)) as AHT", FALSE)
                    ->from("callsDep")
                    ->group_by(array('H'));
                    

            switch( $data['groupBy'] ){
                case 'hora':
                    $this->db->select("CAST(CONCAT(Fecha,' ',CONCAT(HOUR(Hora),IF(MINUTE(Hora)>=30,':30:00',':00:00'))) as DATETIME) as H", FALSE)
                    ->order_by("Fecha, Hora");
                    break;
                case 'dia':
                    $this->db->select("CAST(CONCAT(Fecha,' 00:00:00') as DATETIME) as H", FALSE)
                    ->order_by("Fecha, Hora");
                    break;
                case 'mes':
                    $this->db->select("CAST(CONCAT(YEAR(Fecha),'-',MONTH(Fecha),'-01 00:00:00') as DATETIME) as H", FALSE)
                    ->order_by("Fecha, Hora");
                    break;
                case 'inDay':
                    $this->db->select("CAST(CONCAT('$fin ',CONCAT(HOUR(Hora),IF(MINUTE(Hora)>=30,':30:00',':00:00'))) as DATETIME) as H", FALSE)
                    ->order_by("H");
                    break;
            }
            
            
            if( $a = $this->db->get() ){

                okResponse( "Información correctamente obtenida", 'data', $a->result_array(), $this, 'params', $data );

            }else{
                errResponse('Error en la base de datos', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
            }


        return true;

        });

        jsonPrint( $result );

    }
    
    public function inDeps_get(){

        $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){
            
            $this->db->select("Departamento, id as skill")
                    ->from("PCRCs")
                    ->where("inbound_calls", 1)
                    ->order_by("Departamento");
            
            if( $a = $this->db->get() ){

                okResponse( "Información correctamente obtenida", 'data', $a->result_array(), $this );

            }else{
                errResponse('Error en la base de datos', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
            }


        return true;

        });

        jsonPrint( $result );

    }
    
    private function buildCalls( $fecha, $fechaEnd, $dir = 1, $soporte = 0, $skill = 35, $hora = '' ){
        
        if( $soporte == 1 ){
          $sopor = "HAVING Origin LIKE 'S-%'";
          $ws = "WHEN Canal LIKE '%Soporte%' THEN 
                    CASE
                        WHEN Canal LIKE '%dsk%' THEN
                            CASE 
                                WHEN Canal LIKE '%detalle%' THEN 'S-Desktop Detalle'
                                WHEN Canal LIKE '%pago%' THEN 'S-Desktop Pago'
                                WHEN Canal LIKE '%rsva%' THEN 'S-Desktop Reserva'
                            END 
                        WHEN Canal LIKE '%mob%' THEN
                            CASE 
                                WHEN Canal LIKE '%detalle%' THEN 'S-Mobile Detalle'
                                WHEN Canal LIKE '%pago%' THEN 'S-Mobile Pago'
                                WHEN Canal LIKE '%rsva%' THEN 'S-Mobile Reserva'
                            END
                    END         ";
      }else{
          $sopor = "";
          $ws = "WHEN Canal LIKE '%Soporte%' THEN '800 Soporte'";
      }

        
        $td = $this->db->query("SELECT CURDATE()='$fecha' as today");
        $hoy = $td->row_array();
        
        switch($dir){
            case 1:
                $direction = "AND direction = 1";
                break;
            case 2:
                $direction = "AND direction = 2";
                break;
            case 0:
                $direction = "";
                break;
        }
        
        if( $hora != '' ){
            $hQ = " AND Hora<= '$hora' ";
        }else{
            $hQ = "";
        }
        
        if( $hoy['today'] == 1 ){
            $query = "CREATE TEMPORARY TABLE calls SELECT 
                            IF(callLen IS NULL,
                                'Abandono',
                                'Answered') AS Desconexion,
                            `from` as Llamante,
                            CONCAT(IF( SUBSTRING(dnis,1,1) = '0', \"'\", \"\"),dnis) AS DNIS,
                            Skill, direction,
                            Cola,
                            ivr AS IVR_path,
                            GETIDASESOR(descr_agente, 2) AS asesor,
                            CAST(tstEnter AS DATE) AS Fecha,
                            CAST(tstEnter AS TIME) AS Hora, SEC_TO_TIME(callLen) as Duracion
                        FROM
                            ccexporter.callsDetails a
                                LEFT JOIN
                            Cola_Skill b ON a.queue = b.queue AND b.active=1
                                LEFT JOIN
                            ccexporter.agentDetails c ON a.agent = nome_agente
                        WHERE
                            tstEnter >= CURDATE() 
                        HAVING Skill = $skill $direction $hQ";
        }else{
            $query = "CREATE TEMPORARY TABLE calls SELECT a.*, Skill, direction FROM t_Answered_Calls a LEFT JOIN Cola_Skill b ON a.Cola=b.Cola AND b.active=1 WHERE Fecha BETWEEN '$fecha' AND '$fechaEnd' HAVING Skill=$skill $direction $hQ";
        }
     
        $this->db->query("DROP TEMPORARY TABLE IF EXISTS calls");
        $this->db->query("DROP TEMPORARY TABLE IF EXISTS callsDep");
        $this->db->query($query);
        $this->db->query("CREATE TEMPORARY TABLE callsDep SELECT
            a.*, IF(cc IS NOT NULL, 'mixcoac', dep) AS dep,
            CASE
                WHEN Canal IS NULL THEN
                    CASE
                        WHEN Cola LIKE '%Vuelos%' THEN '800 Vuelos'
                        ELSE '800 Desktop'
                    END
                WHEN Canal LIKE '%Promo Aereo%' THEN '800 Vuelos'
                WHEN Canal LIKE '%Promo' THEN '800 Hoteles'
                WHEN Canal LIKE '%Movil%' THEN '800 Mobile'
                WHEN Canal LIKE '%Cruceros%' THEN '800 Cruceros'
                WHEN Canal LIKE '%Tours%' THEN '800 Tours'
                $ws
                WHEN Canal LIKE '%MX - %' THEN 
                    CASE
                        WHEN Cola LIKE '%Hoteles%' THEN '800 Hoteles'
                        ELSE '800 Destinos'
                    END
                ELSE '800 Desktop'
            END as Origin,
            CASE
                WHEN Cola LIKE '%Vuelos%' THEN
                    CASE
                        WHEN IVR_path = '' THEN 'Vuelos'
                        WHEN IVR_path = '1' THEN 'Vuelos'
                        WHEN IVR_path = '13' THEN 'Vuelos'
                        WHEN IVR_path = '14' THEN 'Otros'
                    END
                WHEN Cola LIKE '%Otros%' THEN
                    CASE
                        WHEN IVR_path = '' THEN 'Otros'
                        WHEN IVR_path = '1' THEN 'Otros'
                        WHEN IVR_path = '13' THEN 'Vuelos'
                        WHEN IVR_path = '14' THEN 'Otros'
                    END
                WHEN Cola LIKE '%Cruceros%' THEN 'Otros'
                WHEN Cola LIKE '%Paquetes%' THEN 
                    CASE
                        WHEN IVR_path = '' THEN 'Paquetes'
                        WHEN IVR_path = '1' THEN 'Paquetes'
                        WHEN IVR_path = '11' THEN 'Hoteles'
                        WHEN IVR_path = '12' THEN 'Paquetes'
                        WHEN IVR_path = '14' THEN 'Otros'
                    END
                WHEN Cola LIKE '%Soporte%' THEN 'Soporte'
                ELSE
                    CASE
                        WHEN IVR_path = '' THEN 'Hoteles'
                        WHEN IVR_path = '1' THEN 'Hoteles'
                        WHEN IVR_path = '11' THEN 'Hoteles'
                        WHEN IVR_path = '12' THEN 'Paquetes'
                        WHEN IVR_path = '14' THEN 'Otros'
                    END
            END as Path,
            CASE
                WHEN dep IS NULL THEN
                    CASE
                        WHEN Desconexion = 'Abandono' THEN 'Abandon'
                        ELSE 'IN'
                    END
                WHEN dep = 29 THEN 
                    CASE
                        WHEN cc IS NOT NULL THEN 'Mixcoac'
                        ELSE 'PDV'
                    END 

                ELSE 'IN'
            END as Grupo
        FROM
            calls a
                LEFT JOIN
            dep_asesores b ON a.asesor = b.asesor
                AND a.Fecha = b.Fecha
                LEFT JOIN
            cc_apoyo c ON a.Fecha BETWEEN inicio AND fin
                AND a.asesor = c.asesor LEFT JOIN Dids d ON a.DNIS = d.DID $sopor");
        
    }

}
