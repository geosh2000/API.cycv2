<?php
//defined('BASEPATH') OR exit('No direct script access allowed');
require( APPPATH.'/libraries/REST_Controller.php');
// use REST_Controller;


class Cuartiles extends REST_Controller {

    public function __construct(){

        parent::__construct();
        $this->load->helper('json_utilities');
        $this->load->helper('validators');
        $this->load->helper('jwt');
        $this->load->database();
    }

    public function getCuartiles_get(){

        $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

        $inicio = $this->uri->segment(3);
        $fin = $this->uri->segment(4);
        $pcrc = (int)$this->uri->segment(5);

        $puestos = array(
                            4   => 47,
                            3   => "33,34",
                            35  => "16,19",
                            5   => 19,
                            6   => 49,
                            7   => 31,
                            8   => 55,
                            9   => "53,52"
                        );

        $hc_puesto = $puestos[$pcrc];

        // Patch para llamadas de ventas MP para hibridos
        if($pcrc == 5){
            $skill = 35;
        }else{
            $skill = $pcrc;
        }

        $result = $this->cuartilesIN( $inicio, $fin, $skill, $hc_puesto );

        return $result;

        });

        $result['data']=$this->quartilize($result['data'], array(
                                                        'MontoTotal'  => 'Asc',
                                                        'AHT'         => 'Desc',
                                                        'FC'          => 'Asc',
                                                        'Colgadas_Relativo' => 'Desc'
                                                        ));


        okResponse( 'Cuartiles Obtenidos', 'data', $result['data'], $this);

    }
        
    public function getCuartilesFecha_get(){

        $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

        $inicio = $this->uri->segment(3);
        $fin = $this->uri->segment(4);
        $pcrc = (int)$this->uri->segment(5);

        $puestos = array(
                            4   => 47,
                            3   => "33,34",
                            35  => "16,19",
                            5   => 19,
                            6   => 49,
                            7   => 31,
                            8   => 55,
                            9   => 53
                        );

        $hc_puesto = $puestos[$pcrc];

        // Patch para llamadas de ventas MP para hibridos
        if($pcrc == 5){
            $skill = 35;
        }else{
            $skill = $pcrc;
        }

        $result = $this->cuartilesINFecha( $inicio, $fin, $skill, $hc_puesto );

        return $result;

        });

        $result['data']=$this->quartilize($result['data'], array(
                                                        'MontoTotal'  => 'Asc',
                                                        'AHT'         => 'Desc',
                                                        'FC'          => 'Asc',
                                                        'Colgadas_Retaltivo' => 'Desc'
                                                        ));


        $this->response($result);

    }

    public function quartilize($array, $fields){


        $result = $array;

        $avgSession = array_sum(array_column($result, 'TotalSesion'))/count($result)*0.7;

        foreach($result as $index => $info){
        foreach($fields as $key => $type){
            if($result[$index]['TotalSesion'] >= $avgSession){
            $data[$key][$index] = $info[$key];
            }
        }
        }

        foreach($data as $key => $info2){

        if($fields[$key] == 'Desc'){
            asort($info2);
        }else{
            arsort($info2);
        }

        $keys = array_keys($info2);

        $length=count($info2);
        $qs = intval($length/4);
        $qsx = ($length % 4)*4;

        $x=1;
        for($i=1; $i<=4; $i++){

            if($i <= $qsx){
            $q[$i] = $qs+1;
            }else{
            $q[$i]=$qs;
            }

        }

        $i=1;
        $x=1;
        foreach($info2 as $key2 => $info3){

            if($x <= $qsx){
            $max = $qs+1;
            }else{
            $max = $qs;
            }

            if($i > $max){
            $x++;
            $i=1;
            }

            $result[$key2][$key.'Q']=$x;

            $i++;
        }
        }

        return $result;

    }

    public function cuartilesIN( $inicio, $fin, $pcrc, $hc_puesto ){
        $this->db->query("DROP TEMPORARY TABLE IF EXISTS queryAsesores");
        $this->db->query("DROP TEMPORARY TABLE IF EXISTS queryLocs");
        $this->db->query("DROP TEMPORARY TABLE IF EXISTS queryLocsB");
        $this->db->query("DROP TEMPORARY TABLE IF EXISTS queryLocsC");
        $this->db->query("DROP TEMPORARY TABLE IF EXISTS queryLocsOK");
        $this->db->query("DROP TEMPORARY TABLE IF EXISTS queryCalls");
        $this->db->query("DROP TEMPORARY TABLE IF EXISTS queryPausas");
        $this->db->query("DROP TEMPORARY TABLE IF EXISTS querySesiones");
        $this->db->query("DROP TEMPORARY TABLE IF EXISTS cuartilOK");

        // Patch para Híbridos MP
        if($hc_puesto == 19){
        $hibridos       = " AND puesto=44 ";
        $hibridosLocs   = " AND tipo=2 ";
        }else{
        $hibridos       = "";
        $hibridosLocs   = "";
        }

        $this->db->query("CREATE TEMPORARY TABLE queryAsesores (SELECT
                            Fecha, vacante, hc_dep, hc_puesto, dep, puesto, asesor, NombreAsesor(asesor,2) as Nombre
                        FROM
                            dep_asesores
                        WHERE
                            hc_puesto IN ($hc_puesto) $hibridos
                                AND Fecha BETWEEN '$inicio' AND '$fin'
                                HAVING vacante IS NOT NULL)");

        $this->db->query("ALTER TABLE queryAsesores ADD PRIMARY KEY (`Fecha`,asesor)");

        $this->db->query("CREATE TEMPORARY TABLE queryLocs SELECT
                            b.*, IF(VentaMXN!=0,Localizador,NULL) as NewLoc
                        FROM
                            (SELECT DISTINCT
                                asesor
                            FROM
                                queryAsesores) a
                                RIGHT JOIN
                            (SELECT
                                *
                            FROM
                                t_Locs
                            WHERE
                                Fecha BETWEEN '$inicio' AND '$fin' AND asesor>0 AND asesor IS NOT NULL $hibridosLocs) b ON a.asesor = b.asesor");

        $this->db->query("ALTER TABLE queryLocs ADD PRIMARY KEY (Fecha, Hora, Localizador, VentaMXN)");

        $this->db->query("CREATE TEMPORARY TABLE queryLocsB SELECT * FROM queryLocs");
        $this->db->query("ALTER TABLE queryLocsB ADD PRIMARY KEY (Fecha, Hora, Localizador, VentaMXN)");

        $this->db->query("CREATE TEMPORARY TABLE queryLocsC SELECT * FROM queryLocs");
        $this->db->query("ALTER TABLE queryLocsC ADD PRIMARY KEY (Fecha, Hora, Localizador, VentaMXN)");

        $this->db->query("CREATE TEMPORARY TABLE queryLocsOK SELECT
                            a.*,
                            FinalBalance,
                            IF(FinalBalance > 0 AND NewLoc IS NOT NULL,
                                NewLoc,
                                NULL) AS NewLocPositive,
                            IF(periodCreated IS NOT NULL,
                                a.Localizador,
                                NULL) AS periodCreated
                        FROM
                            queryLocs a
                                LEFT JOIN
                            (SELECT
                                Localizador,
                                    SUM(VentaMXN + OtrosIngresosMXN + EgresosMXN) AS FinalBalance
                            FROM
                                queryLocsB
                            GROUP BY Localizador) b ON a.Localizador = b.Localizador
                                LEFT JOIN
                            (SELECT DISTINCT
                                NewLoc AS periodCreated
                            FROM
                                queryLocsC) c ON a.Localizador = c.periodCreated");

        $this->db->query("ALTER TABLE queryLocsOK ADD PRIMARY KEY (Fecha, Hora, Localizador, VentaMXN)");

        $this->db->query("CREATE TEMPORARY TABLE queryCalls
                        SELECT
                            a.*, Skill
                        FROM
                            t_Answered_Calls a
                            LEFT JOIN
                            Cola_Skill b ON a.Cola = b.Cola
                        WHERE
                            Fecha BETWEEN '$inicio' AND '$fin'
                        HAVING Skill = $pcrc");

        $this->db->query("ALTER TABLE queryCalls ADD PRIMARY KEY (Fecha, Hora, `Llamante`(15), `AsteriskId`(25))");

        $this->db->query("CREATE TEMPORARY TABLE queryPausas SELECT
                            a.asesor,
                            SUM(IF(Productiva=0,TIME_TO_SEC(Duracion),0)) as PNP,
                            SUM(IF(Productiva=1,TIME_TO_SEC(Duracion),0)) as PP
                        FROM
                            asesores_pausas a
                                LEFT JOIN
                            (SELECT
                                asesor
                            FROM
                                queryAsesores
                            GROUP BY Nombre) b ON a.asesor = b.asesor
                            LEFT JOIN Tipos_pausas c ON a.tipo=c.pausa_id
                        WHERE
                            Inicio BETWEEN '$inicio 00:00:00' AND '$fin 23:59:59'
                                AND b.asesor IS NOT NULL
                                AND Skill = $pcrc
                                GROUP BY a.asesor");

        $this->db->query("ALTER TABLE queryPausas  ADD PRIMARY KEY (asesor)");

        $this->db->query("CREATE TEMPORARY TABLE querySesiones SELECT
                            a.asesor, SUM(TIME_TO_SEC(Duracion)) AS Sesion
                        FROM
                            t_Sesiones a
                                LEFT JOIN
                            (SELECT
                                asesor
                            FROM
                                queryAsesores
                            GROUP BY Nombre) b ON a.asesor = b.asesor
                        WHERE
                            Fecha_in BETWEEN '$inicio' AND '$fin'
                                AND Skill = $pcrc
                                AND b.asesor IS NOT NULL
                        GROUP BY a.asesor");

        $this->db->query("ALTER TABLE querySesiones ADD PRIMARY KEY (asesor)");

        $this->db->query("CREATE TEMPORARY TABLE cuartilOK SELECT
                            a.Nombre,
                            Usuario as user,
                            NOMBREASESOR(GETIDASESOR(FINDSUPDAY(a.asesor, '$fin'), 2),
                                    2) AS Supervisor,
                            NewLocsPositive as LocsPeriodo,
                            NewLocsPositive / Total_Llamadas_Real AS FC,
                            Sesion as TotalSesion,
                            Total_Llamadas_Real,
                            (Sesion-PNP)/Sesion as Utilizacion,
                            PNP,
                            Sesion,
                            MontoPeriodo,
                            MontoNoPeriodo,
                            MontoTotal,
                            ShortCalls as ShortCalls_Absoluto,
                            ShortCalls/Total_Llamadas as ShortCalls_Relativo,
                            Colgadas as Colgadas_Absoluto,
                            Colgadas/Total_Llamadas as Colgadas_Relativo,
                            AHT,
                            PP as ACW_Absoluto,
                            PP/Sesion as ACW_Relativo
                        FROM
                            (SELECT
                                *
                            FROM
                                queryAsesores
                            GROUP BY Nombre) a
                                LEFT JOIN
                            (SELECT
                                asesor,
                                    COUNT(DISTINCT Localizador) AS Locs,
                                    COUNT(DISTINCT NewLoc) AS LocsNuevos,
                                    COUNT(DISTINCT NewLocPositive) AS NewLocsPositive,
                                    SUM(IF(periodCreated IS NOT NULL, VentaMXN + OtrosIngresosMXN + EgresosMXN, 0)) AS MontoPeriodo,
                                    SUM(VentaMXN + OtrosIngresosMXN + EgresosMXN) - SUM(IF(periodCreated IS NOT NULL, VentaMXN + OtrosIngresosMXN + EgresosMXN, 0)) AS MontoNoPeriodo,
                                    SUM(VentaMXN + OtrosIngresosMXN + EgresosMXN) AS MontoTotal
                            FROM
                                queryLocsOK
                            GROUP BY asesor) b ON a.asesor = b.asesor
                                LEFT JOIN
                            (SELECT
                                asesor,
                                    COUNT(ac_id) AS Total_Llamadas,
                                    COUNT(IF(Desconexion = 'Agente', ac_id, NULL)) as Colgadas,
                                    COUNT(IF(Desconexion = 'Transferida'
                                        AND Duracion_Real <= '00:02:00', ac_id, NULL)) AS ShortCalls,
                                    COUNT(ac_id) - COUNT(IF(Desconexion = 'Transferida'
                                        AND Duracion_Real <= '00:02:00', ac_id, NULL)) AS Total_Llamadas_Real,
                                    AVG(TIME_TO_SEC(Duracion_Real)) AS AHT,
                                    SUM(TIME_TO_SEC(Duracion_Real)) AS TalkingTime
                            FROM
                                queryCalls
                            GROUP BY asesor
                            HAVING asesor IS NOT NULL) c ON a.asesor = c.asesor
                                LEFT JOIN
                            querySesiones d ON a.asesor = d.asesor
                                LEFT JOIN
                            queryPausas e ON a.asesor = e.asesor
                                LEFT JOIN
                            Asesores f ON a.asesor=f.id");

        if($query = $this->db->get('cuartilOK')){
        return  array(
                        'status'  => true,
                        'data'    => $query->result_array(),
                        'msg'     => "Cuartiles cargados correctamente"
                        );
        }else{
        errResponse( "No es posible consultar las excepciones, no se actualizó nada", REST_Controller::HTTP_BAD_REQUEST, $this, 'errores', $this->db->error() );
        }

        return $result;
    }
        
    public function cuartilesINFecha( $inicio, $fin, $pcrc, $hc_puesto ){
        $this->db->query("DROP TEMPORARY TABLE IF EXISTS queryAsesores");
        $this->db->query("DROP TEMPORARY TABLE IF EXISTS queryLocs");
        $this->db->query("DROP TEMPORARY TABLE IF EXISTS queryLocsB");
        $this->db->query("DROP TEMPORARY TABLE IF EXISTS queryLocsC");
        $this->db->query("DROP TEMPORARY TABLE IF EXISTS queryLocsOK");
        $this->db->query("DROP TEMPORARY TABLE IF EXISTS queryCalls");
        $this->db->query("DROP TEMPORARY TABLE IF EXISTS queryPausas");
        $this->db->query("DROP TEMPORARY TABLE IF EXISTS querySesiones");
        $this->db->query("DROP TEMPORARY TABLE IF EXISTS cuartilOK");

        // Patch para Híbridos MP
        if($hc_puesto == 19){
        $hibridos       = " AND puesto=44 ";
        $hibridosLocs   = " AND tipo=2 ";
        }else{
        $hibridos       = "";
        $hibridosLocs   = "";
        }

        $this->db->query("CREATE TEMPORARY TABLE queryAsesores (SELECT
                            Fecha,
                            b.id as vacante, hc_dep, hc_puesto, departamento as dep, puesto,
                            GETVACANTE(id, Fecha) as asesor, NombreAsesor(GETVACANTE(id, Fecha),2) as Nombre
                        FROM
                            Fechas a
                                LEFT JOIN
                            asesores_plazas b ON a.Fecha BETWEEN inicio AND fin
                        WHERE
                            hc_puesto IN ($hc_puesto) $hibridos
                                AND Fecha BETWEEN '$inicio' AND '$fin'
                                HAVING asesor IS NOT NULL)");

        $this->db->query("ALTER TABLE queryAsesores ADD PRIMARY KEY (`Fecha`,asesor)");

        $this->db->query("CREATE TEMPORARY TABLE queryLocs SELECT
                            b.*, IF(VentaMXN!=0,Localizador,NULL) as NewLoc
                        FROM
                            (SELECT DISTINCT
                                asesor
                            FROM
                                queryAsesores) a
                                RIGHT JOIN
                            (SELECT
                                *
                            FROM
                                t_Locs
                            WHERE
                                Fecha BETWEEN '$inicio' AND '$fin' AND asesor>0 AND asesor IS NOT NULL $hibridosLocs) b ON a.asesor = b.asesor");

        $this->db->query("ALTER TABLE queryLocs ADD PRIMARY KEY (Fecha, Hora, Localizador, VentaMXN)");

        $this->db->query("CREATE TEMPORARY TABLE queryLocsB SELECT * FROM queryLocs");
        $this->db->query("ALTER TABLE queryLocsB ADD PRIMARY KEY (Fecha, Hora, Localizador, VentaMXN)");

        $this->db->query("CREATE TEMPORARY TABLE queryLocsC SELECT * FROM queryLocs");
        $this->db->query("ALTER TABLE queryLocsC ADD PRIMARY KEY (Fecha, Hora, Localizador, VentaMXN)");

        $this->db->query("CREATE TEMPORARY TABLE queryLocsOK SELECT
                            a.*,
                            FinalBalance,
                            IF(FinalBalance > 0 AND NewLoc IS NOT NULL,
                                NewLoc,
                                NULL) AS NewLocPositive,
                            IF(periodCreated IS NOT NULL,
                                a.Localizador,
                                NULL) AS periodCreated
                        FROM
                            queryLocs a
                                LEFT JOIN
                            (SELECT
                                Fecha, Localizador,
                                    SUM(VentaMXN + OtrosIngresosMXN + EgresosMXN) AS FinalBalance
                            FROM
                                queryLocsB
                            GROUP BY Fecha, Localizador) b ON a.Fecha = b.Fecha AND a.Localizador = b.Localizador
                                LEFT JOIN
                            (SELECT DISTINCT
                                Fecha, NewLoc AS periodCreated
                            FROM
                                queryLocsC GROUP BY Fecha, NewLoc) c ON a.Fecha = c.Fecha AND a.Localizador = c.periodCreated");

        $this->db->query("ALTER TABLE queryLocsOK ADD PRIMARY KEY (Fecha, Hora, Localizador, VentaMXN)");

        $this->db->query("CREATE TEMPORARY TABLE queryCalls
                        SELECT
                            a.*, Skill
                        FROM
                            t_Answered_Calls a
                            LEFT JOIN
                            Cola_Skill b ON a.Cola = b.Cola
                        WHERE
                            Fecha BETWEEN '$inicio' AND '$fin'
                        HAVING Skill = $pcrc");

        $this->db->query("ALTER TABLE queryCalls ADD PRIMARY KEY (Fecha, Hora, `Llamante`(15), `AsteriskId`(25))");

        $this->db->query("CREATE TEMPORARY TABLE queryPausas SELECT
                            CAST(Inicio as DATE) as Fecha,
                            a.asesor,
                            SUM(IF(Productiva=0,TIME_TO_SEC(Duracion),0)) as PNP,
                            SUM(IF(Productiva=1,TIME_TO_SEC(Duracion),0)) as PP
                        FROM
                            asesores_pausas a
                                LEFT JOIN
                            (SELECT
                                asesor
                            FROM
                                queryAsesores
                            GROUP BY Nombre) b ON a.asesor = b.asesor
                            LEFT JOIN Tipos_pausas c ON a.tipo=c.pausa_id
                        WHERE
                            Inicio BETWEEN '$inicio 00:00:00' AND '$fin 23:59:59'
                                AND b.asesor IS NOT NULL
                                AND Skill = $pcrc
                                GROUP BY Fecha, a.asesor");

        $this->db->query("ALTER TABLE queryPausas  ADD PRIMARY KEY (Fecha, asesor)");

        $this->db->query("CREATE TEMPORARY TABLE querySesiones SELECT
                            CAST(login as DATE) as Fecha, a.asesor, SUM(TIME_TO_SEC(duracion)) AS Sesion
                        FROM
                            asesores_logs a
                                LEFT JOIN
                            (SELECT
                                asesor
                            FROM
                                queryAsesores
                            GROUP BY Nombre) b ON a.asesor = b.asesor
                        WHERE
                            login BETWEEN '$inicio 00:00:00' AND '$fin 23:59:59'
                                AND skill = $pcrc
                                AND b.asesor IS NOT NULL
                        GROUP BY Fecha, a.asesor");

        $this->db->query("ALTER TABLE querySesiones ADD PRIMARY KEY (Fecha, asesor)");

        $this->db->query("CREATE TEMPORARY TABLE cuartilOK SELECT
                            a.Fecha,
                            a.Nombre,
                            Usuario as user,
                            NOMBREASESOR(GETIDASESOR(FINDSUPDAY(a.asesor, '$fin'), 2),
                                    2) AS Supervisor,
                            NewLocsPositive as LocsPeriodo,
                            NewLocsPositive / Total_Llamadas_Real AS FC,
                            Sesion as TotalSesion,
                            Total_Llamadas_Real,
                            (Sesion-PNP)/Sesion as Utilizacion,
                            PNP,
                            Sesion,
                            MontoPeriodo,
                            MontoNoPeriodo,
                            MontoTotal,
                            ShortCalls as ShortCalls_Absoluto,
                            ShortCalls/Total_Llamadas as ShortCalls_Relativo,
                            Colgadas as Colgadas_Absoluto,
                            Colgadas/Total_Llamadas as Colgadas_Relativo,
                            AHT,
                            PP as ACW_Absoluto,
                            PP/Sesion as ACW_Relativo
                        FROM
                            (SELECT
                                *
                            FROM
                                queryAsesores
                            GROUP BY Fecha, Nombre) a
                                LEFT JOIN
                            (SELECT
                                Fecha, asesor,
                                    COUNT(DISTINCT Localizador) AS Locs,
                                    COUNT(DISTINCT NewLoc) AS LocsNuevos,
                                    COUNT(DISTINCT NewLocPositive) AS NewLocsPositive,
                                    SUM(IF(periodCreated IS NOT NULL, VentaMXN + OtrosIngresosMXN + EgresosMXN, 0)) AS MontoPeriodo,
                                    SUM(VentaMXN + OtrosIngresosMXN + EgresosMXN) - SUM(IF(periodCreated IS NOT NULL, VentaMXN + OtrosIngresosMXN + EgresosMXN, 0)) AS MontoNoPeriodo,
                                    SUM(VentaMXN + OtrosIngresosMXN + EgresosMXN) AS MontoTotal
                            FROM
                                queryLocsOK
                            GROUP BY Fecha, asesor) b ON a.Fecha = b.Fecha AND a.asesor = b.asesor
                                LEFT JOIN
                            (SELECT
                                Fecha, asesor,
                                    COUNT(ac_id) AS Total_Llamadas,
                                    COUNT(IF(Desconexion = 'Agente', ac_id, NULL)) as Colgadas,
                                    COUNT(IF(Desconexion = 'Transferida'
                                        AND Duracion_Real <= '00:02:00', ac_id, NULL)) AS ShortCalls,
                                    COUNT(ac_id) - COUNT(IF(Desconexion = 'Transferida'
                                        AND Duracion_Real <= '00:02:00', ac_id, NULL)) AS Total_Llamadas_Real,
                                    AVG(TIME_TO_SEC(Duracion_Real)) AS AHT,
                                    SUM(TIME_TO_SEC(Duracion_Real)) AS TalkingTime
                            FROM
                                queryCalls
                            GROUP BY Fecha, asesor
                            HAVING asesor IS NOT NULL) c ON a.Fecha = c.Fecha AND a.asesor = c.asesor
                                LEFT JOIN
                            querySesiones d ON a.Fecha = d.Fecha AND a.asesor = d.asesor
                                LEFT JOIN
                            queryPausas e ON a.Fecha = e.Fecha AND a.asesor = e.asesor
                                LEFT JOIN
                            Asesores f ON a.asesor=f.id");

        if($query = $this->db->get('cuartilOK')){
        return  array(
                        'status'  => true,
                        'data'    => $query->result_array(),
                        'msg'     => "Cuartiles cargados correctamente"
                        );
        }else{
        errResponse( "No es posible consultar las excepciones, no se actualizó nada", REST_Controller::HTTP_BAD_REQUEST, $this, 'errores', $this->db->error() );
        }

        return $result;
    }
    

    public function dataMonitorAsesores_get(){

        $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){
            
            $super = $this->uri->segment(3);
            
            $val = $this->db->query("SELECT COUNT(*) as s FROM graf_dailySale WHERE supMes = $super");
            $v = $val->row_array();
                    
            
        $this->db->select("a.asesor, a.Fecha, supervisor, supMes, a.dep, MontoAll as monto_total, 
                            HotelInAll + HotelNotInAll as monto_hotel,
                            VueloInAll + VueloNotInAll as monto_vuelo,
                            PaqueteInAll + PaqueteNotInAll as monto_paquete,
                            TourInAll + TourNotInAll as monto_tours,
                            TransferInAll + TransferNotInAll as monto_transfer,
                            MontoAll - ((HotelInAll + HotelNotInAll) + (VueloInAll + VueloNotInAll) + (PaqueteInAll + PaqueteNotInAll) + (TourInAll + TourNotInAll) + (TransferInAll + TransferNotInAll)) as monto_otros,
                            LocsIn, LocsNotIn, LocsIn+LocsNotIn as rsvas_total,
                            callsIn as llamadas_total, callsOut as llamadasOut,
                            TTIn as talking_time, TTOut as talking_timeOut, intentosOut as intentos, a.Last_Update, NOMBREASESOR(a.asesor,1) as nombre, NOMBREASESOR(a.asesor, 5) as colab, vacante", FALSE)
                ->from('graf_dailySale a')
                ->join('dep_asesores b', 'a.asesor=b.asesor AND CURDATE()=b.Fecha', 'left', FALSE)
                ->where("a.Fecha BETWEEN ", "CAST(CONCAT(YEAR(CURDATE()),'-',MONTH(CURDATE()),'-01') as DATE) AND CURDATE()", FALSE)
                ->where("supMes", $super)
                ->where("b.dep !=",35)
                ->where("b.dep !=",5)
                ->having('vacante IS NOT', ' NULL', FALSE)
                ->order_by('nombre');


        if($q = $this->db->get()){
            

            okResponse( 'Data Obtenida', 'data', $q->result_array(), $this );

        }else{

            errResponse('Error en la base de datos', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());

        }

        return true;

        });

        jsonPrint( $result );

    }
    
    public function dataMonitorMetas_get(){

        $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){
            
            $skill = $this->uri->segment(3);

        $this->db->select("*")
                ->from('metas')
                ->where("mes ", "MONTH(CURDATE())", FALSE)
                ->where("anio ", "YEAR(CURDATE())", FALSE)
                ->where("skill", $skill);


        if($q = $this->db->get()){
            

            okResponse( 'Data Obtenida', 'data', $q->row_array(), $this );

        }else{

            errResponse('Error en la base de datos', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());

        }

        return true;

        });

        jsonPrint( $result );

    }
    
    public function cuartiles_get(){

        $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

            // =================================================
            // START GET URI DATA
            // =================================================
                $inicio = $this->uri->segment(3);
                $fin = $this->uri->segment(4);
                $skill = $this->uri->segment(5);
                $p_sv = $this->uri->segment(6) == "true" ? TRUE : FALSE;
                $p_paq = $this->uri->segment(7) == "true" ? TRUE : FALSE;
            
                // =================================================
                // START DEFINE SV/ALL
                // =================================================
                    if( $p_sv ){
                        $sv = 'SV';
                    }else{
                        $sv = 'All';
                    }
                // =================================================
                // END DEFINE SV/ALL
                // =================================================
                
                // =================================================
                // START DEFINE PAQ/ALL
                // =================================================
                    if( $p_paq ){
                        $paq = '';
                    }else{
                        $paq = 'All';
                    }
                // =================================================
                // END DEFINE PAQ/ALL
                // =================================================
            // =================================================
            // END GET URI DATA
            // =================================================

            // =================================================
            // START PARAMETROS
            // =================================================
                $this->db->query("SET @inicio = CAST('$inicio' as DATE)");
                $this->db->query("SET @fin = CAST('$fin' as DATE)");
                $this->db->query("SET @skill = $skill");
            // =================================================
            // END PARAMETROS
            // =================================================
            
            // =================================================
            // START PAUSAS
            // =================================================
                $this->db->query("DROP TEMPORARY TABLE IF EXISTS pausasRAW");
                $this->db->query("CREATE TEMPORARY TABLE pausasRAW SELECT 
                    a.Fecha,
                    a.asesor,
                    dep,
                    a.esquema_vacante,
                    IF(correctPauseType IS NOT NULL,
                        correctPauseType,
                        tipo) AS tipoOK,
                    COALESCE(TIMEDIFF(IF(Inicio<js,IF(Fin>js,Fin,NULL),IF(Inicio<je,IF(Fin>je,je,Fin),NULL)),IF(Inicio<js,IF(Fin>js,js,NULL),IF(Inicio<je,Inicio, Null))),'00:00:00') as Duracion,
                    COALESCE(c.status, 0) AS st
                FROM
                    dep_asesores a
                        LEFT JOIN
                    asesores_pausas b ON a.asesor = b.asesor
                        AND a.Fecha = CAST(b.inicio AS DATE)
                        LEFT JOIN
                    asesores_pausas_status c ON b.id = c.id
                        LEFT JOIN
                    asesores_programacion d ON a.asesor = d.asesor
                        AND a.Fecha = d.Fecha
                WHERE
                    a.Fecha BETWEEN @inicio AND @fin
                        AND dep IN (3 , 52, 5, 35, 7, 8, 9, 4, 6, 10, 2, 50)
                        AND vacante IS NOT NULL
                        AND b.id IS NOT NULL
                        AND (c.status IS NULL OR c.status != 2)
                        AND (tipo IN (3 , 11)
                        AND (correctPauseType IS NULL
                        OR correctPauseType IN (3 , 11))
                        OR correctPauseType IN (3 , 11))");

                $this->db->query("DROP TEMPORARY TABLE IF EXISTS pausas");
                $this->db->query("CREATE TEMPORARY TABLE pausas
                SELECT 
                    Fecha,
                    asesor,
                    TIME_TO_SEC(TIMEDIFF(CHECKLOG(Fecha,asesor,'out'),CHECKLOG(Fecha,asesor,'in'))) as Sesion,
                    SUM(IF(tipoOK = 3, TIME_TO_SEC(Duracion), 0)) AS Comida,
                    SUM(IF(tipoOK = 11,
                        TIME_TO_SEC(Duracion),
                        0)) AS PNP,
                    IF( st=0, IF( SUM(IF(tipoOK = 3, TIME_TO_SEC(Duracion), 0)) > 
                                CASE
                                    WHEN dep IN ( 10, 8 ) THEN 3600+59
                                    WHEN dep IN ( 5 ) THEN 2400+59
                                    ELSE 1800+59
                                END OR
                        SUM(IF(tipoOK = 11,
                        TIME_TO_SEC(Duracion),
                        0)) > 
                                CASE
                                    WHEN esquema_vacante = 4 THEN 780+59
                                    WHEN esquema_vacante = 6 THEN 1800+59
                                    WHEN esquema_vacante = 8 THEN 1020+59
                                    WHEN esquema_vacante = 10 THEN 1260+59
                                END , 1 , 0),0) as Exceed
                FROM
                    pausasRAW
                GROUP BY Fecha , asesor");

                $this->db->query("ALTER TABLE pausas ADD PRIMARY KEY (Fecha, asesor)");
            // =================================================
            // END PAUSAS
            // =================================================
            
            // =================================================
            // START PYA
            // =================================================
                $this->db->query("DROP TEMPORARY TABLE IF EXISTS pyaRAW");
                $this->db->query("CREATE TEMPORARY TABLE pyaRAW SELECT 
                    b.Fecha,
                    b.asesor,
                    js,
                    je,
                    CHECKLOG(b.Fecha, b.asesor, 'in') as login,
                    CHECKLOG(b.Fecha, b.asesor, 'out') as logout,
                    ausentismo, tipo
                FROM
                    asesores_programacion a
                        RIGHT JOIN
                    dep_asesores b ON a.asesor = b.asesor
                        AND a.Fecha = b.Fecha
                        LEFT JOIN
                    asesores_ausentismos aus ON b.Fecha = aus.Fecha
                        AND b.asesor = aus.asesor
                        LEFT JOIN
                    asesores_pya_exceptions p ON b.asesor = p.asesor
                        AND p.Fecha = b.Fecha
                WHERE
                    b.Fecha BETWEEN @inicio AND @fin
                        AND dep IN (@skill)
                        AND vacante IS NOT NULL");

                $this->db->query("DROP TEMPORARY TABLE IF EXISTS pya");
                $this->db->query("CREATE TEMPORARY TABLE pya SELECT 
                    Fecha, asesor,
                    IF(ausentismo NOT IN (22 , 15)
                            OR ausentismo IS NULL,
                        IF(js != je AND login IS NULL,
                            IF(ausentismo IN (8 , 15, 22)
                                    OR ausentismo IS NULL,
                                1,
                                0),
                            IF(ausentismo != 15 OR ausentismo IS NULL,
                                IF(logout < je,
                                    IF(TIME_TO_SEC(TIMEDIFF(logout, js)) / TIME_TO_SEC(TIMEDIFF(je, js)) < 0.6,
                                        IF(ausentismo IN (8 , 15, 22)
                                                OR ausentismo IS NULL,
                                            1,
                                            0),
                                        0),
                                    0),
                                1)),
                        1) AS FA,
                    IF(js != je AND login IS NOT NULL
                            AND (ausentismo IS NULL OR ausentismo = 10),
                        IF(login >= ADDTIME(js, '00:01:00') AND login < ADDTIME(js, '00:13:00'),
                            IF(tipo != 3 OR tipo IS NULL, 1, 0),
                            0),
                        0) AS RTA,
                    IF(js != je AND login IS NOT NULL
                            AND (ausentismo IS NULL OR ausentismo = 10),
                        IF(login >= ADDTIME(js, '00:13:00'),
                            IF(tipo != 3 OR tipo IS NULL, 1, 0),
                            0),
                        0) AS RTB
                FROM
                    pyaRAW");
                $this->db->query("ALTER TABLE pya ADD PRIMARY KEY (Fecha, asesor)");
            // =================================================
            // END PYA
            // =================================================
            
            // =================================================
            // START BO
            // =================================================
                $isBoQ = $this->db->select('isBO')->from('PCRCs')->where('id', $skill)->get();
                $isBoR = $isBoQ->row_array();
                $isBo = false;
                // okResponse('data','data',$isBoR,$this);
                if( $isBoR['isBO'] == "1" ){ $isBo = true; }


                if( $isBo == true ){
                    
                    $this->db->query("DROP TEMPORARY TABLE IF EXISTS boSes");
                    $this->db->query("CREATE TEMPORARY TABLE boSes
                    SELECT 
                        Fecha, a.asesor, 
                        SUM(IF(skill=37,TIME_TO_SEC(duracion),0))/60/60 as Mailing,
                        SUM(IF(skill=38,TIME_TO_SEC(duracion),0))/60/60 as Confirming,
                        SUM(IF(skill=39,TIME_TO_SEC(duracion),0))/60/60 as Reembolsos,
                        SUM(IF(skill=40,TIME_TO_SEC(duracion),0))/60/60 as MejoraContinua,
                        SUM(IF(skill=45,TIME_TO_SEC(duracion),0))/60/60 as AgenciasConfirming,
                        SUM(IF(skill=48,TIME_TO_SEC(duracion),0))/60/60 as Afectaciones,
                        SUM(IF(skill=49,TIME_TO_SEC(duracion),0))/60/60 as AgenciasMejora
                    FROM
                        asesores_logs a
                            LEFT JOIN
                        dep_asesores b ON a.asesor = b.asesor
                            AND CAST(login AS DATE) = b.Fecha
                    WHERE
                        login BETWEEN @inicio AND ADDDATE(@fin, 1)
                            AND dep = $skill
                            AND vacante IS NOT NULL 
                            AND puesto != 11
                    GROUP BY a.asesor, Fecha");
                    $this->db->query("ALTER TABLE boSes ADD PRIMARY KEY (asesor, Fecha)");

                    $this->db->query("DROP TEMPORARY TABLE IF EXISTS boCases");
                    $this->db->query("CREATE TEMPORARY TABLE boCases
                    SELECT 
                        c.id AS asesor,
                        a.id,
                        a.area as areaid,
                        b.area,
                        Nombre,
                        fecha_recepcion,
                        em,
                        localizador,
                        a.`status` as st,
                        date_created,
                        internal_id,
                        bo_skill
                    FROM
                        bo_tipificacion a
                            LEFT JOIN
                        bo_areas b ON a.area = b.bo_area_id
                            LEFT JOIN
                        Asesores c ON a.asesor = c.id
                    WHERE
                        a.date_created BETWEEN @inicio AND ADDDATE(@fin,1)");

                    $this->db->query("DROP TEMPORARY TABLE IF EXISTS boCaseSum");
                    $this->db->query("CREATE TEMPORARY TABLE boCaseSum
                    SELECT 
                        CAST(date_created AS DATE) AS FechaBO,
                        a.asesor as asesorBO,
                        COUNT(IF(areaid = 1 AND st IN (8), id, NULL)) AS Confirming_Spam,
                        COUNT(IF(areaid = 1 AND st IN (7), id, NULL)) AS Confirming_Escalado,
                        COUNT(IF(areaid = 1 AND st IN (1,17,22,32), id, NULL)) AS Confirming_SeguimientoCliente,
                        COUNT(IF(areaid = 1 AND st IN (3,20,24,30), id, NULL)) AS Confirming_SeguimientoInterno,
                        COUNT(IF(areaid = 1 AND st IN (2,18,23,31), id, NULL)) AS Confirming_SeguimientoProveedor,
                        COUNT(IF(areaid = 1 AND st IN (6), id, NULL)) AS Confirming_ResueltoBO,
                        COUNT(IF(areaid = 1 AND st IN (27), id, NULL)) AS Confirming_ResueltoIN,
                        COUNT(IF(areaid = 1 AND st IN (5,19,25,34), id, NULL)) AS Confirming_FinBO,
                        COUNT(IF(areaid = 1 AND st IN (26,28,29,33), id, NULL)) AS Confirming_FinIN,
                        COUNT(IF(areaid = 1 AND st!=8, id, NULL)) AS Confirming_Total,

                        COUNT(IF(areaid = 2 AND st IN (8), id, NULL)) AS Mailing_Spam,
                        COUNT(IF(areaid = 2 AND st IN (7), id, NULL)) AS Mailing_Escalado,
                        COUNT(IF(areaid = 2 AND st IN (1,17,22,32), id, NULL)) AS Mailing_SeguimientoCliente,
                        COUNT(IF(areaid = 2 AND st IN (3,20,24,30), id, NULL)) AS Mailing_SeguimientoInterno,
                        COUNT(IF(areaid = 2 AND st IN (2,18,23,31), id, NULL)) AS Mailing_SeguimientoProveedor,
                        COUNT(IF(areaid = 2 AND st IN (6), id, NULL)) AS Mailing_ResueltoBO,
                        COUNT(IF(areaid = 2 AND st IN (27), id, NULL)) AS Mailing_ResueltoIN,
                        COUNT(IF(areaid = 2 AND st IN (5,19,25,34), id, NULL)) AS Mailing_FinBO,
                        COUNT(IF(areaid = 2 AND st IN (26,28,29,33), id, NULL)) AS Mailing_FinIN,
                        COUNT(IF(areaid = 2 AND st!=8, id, NULL)) AS Mailing_Total,

                        COUNT(IF(areaid = 3 AND st IN (8), id, NULL)) AS MC_Spam,
                        COUNT(IF(areaid = 3 AND st IN (7), id, NULL)) AS MC_Escalado,
                        COUNT(IF(areaid = 3 AND st IN (1,17,22,32), id, NULL)) AS MC_SeguimientoCliente,
                        COUNT(IF(areaid = 3 AND st IN (3,20,24,30), id, NULL)) AS MC_SeguimientoInterno,
                        COUNT(IF(areaid = 3 AND st IN (2,18,23,31), id, NULL)) AS MC_SeguimientoProveedor,
                        COUNT(IF(areaid = 3 AND st IN (6), id, NULL)) AS MC_ResueltoBO,
                        COUNT(IF(areaid = 3 AND st IN (27), id, NULL)) AS MC_ResueltoIN,
                        COUNT(IF(areaid = 3 AND st IN (5,19,25,34), id, NULL)) AS MC_FinBO,
                        COUNT(IF(areaid = 3 AND st IN (26,28,29,33), id, NULL)) AS MC_FinIN,
                        COUNT(IF(areaid = 3 AND st!=8, id, NULL)) AS MC_Total,

                        COUNT(IF(areaid = 4 AND st IN (8), id, NULL)) AS Reembolsos_Spam,
                        COUNT(IF(areaid = 4 AND st IN (7), id, NULL)) AS Reembolsos_Escalado,
                        COUNT(IF(areaid = 4 AND st IN (1,17,22,32), id, NULL)) AS Reembolsos_SeguimientoCliente,
                        COUNT(IF(areaid = 4 AND st IN (3,20,24,30), id, NULL)) AS Reembolsos_SeguimientoInterno,
                        COUNT(IF(areaid = 4 AND st IN (2,18,23,31), id, NULL)) AS Reembolsos_SeguimientoProveedor,
                        COUNT(IF(areaid = 4 AND st IN (6), id, NULL)) AS Reembolsos_ResueltoBO,
                        COUNT(IF(areaid = 4 AND st IN (27), id, NULL)) AS Reembolsos_ResueltoIN,
                        COUNT(IF(areaid = 4 AND st IN (5,19,25,34), id, NULL)) AS Reembolsos_FinBO,
                        COUNT(IF(areaid = 4 AND st IN (26,28,29,33), id, NULL)) AS Reembolsos_FinIN,
                        COUNT(IF(areaid = 4 AND st!=8, id, NULL)) AS Reembolsos_Total,

                        COUNT(IF(areaid = 5 AND st IN (8), id, NULL)) AS AgenciasConfirming_Spam,
                        COUNT(IF(areaid = 5 AND st IN (7), id, NULL)) AS AgenciasConfirming_Escalado,
                        COUNT(IF(areaid = 5 AND st IN (1,17,22,32), id, NULL)) AS AgenciasConfirming_SeguimientoCliente,
                        COUNT(IF(areaid = 5 AND st IN (3,20,24,30), id, NULL)) AS AgenciasConfirming_SeguimientoInterno,
                        COUNT(IF(areaid = 5 AND st IN (2,18,23,31), id, NULL)) AS AgenciasConfirming_SeguimientoProveedor,
                        COUNT(IF(areaid = 5 AND st IN (6), id, NULL)) AS AgenciasConfirming_ResueltoBO,
                        COUNT(IF(areaid = 5 AND st IN (27), id, NULL)) AS AgenciasConfirming_ResueltoIN,
                        COUNT(IF(areaid = 5 AND st IN (5,19,25,34), id, NULL)) AS AgenciasConfirming_FinBO,
                        COUNT(IF(areaid = 5 AND st IN (26,28,29,33), id, NULL)) AS AgenciasConfirming_FinIN,
                        COUNT(IF(areaid = 5 AND st!=8, id, NULL)) AS AgenciasConfirming_Total,

                        COUNT(IF(areaid = 7 AND st IN (8), id, NULL)) AS AgenciasMejora_Spam,
                        COUNT(IF(areaid = 7 AND st IN (7), id, NULL)) AS AgenciasMejora_Escalado,
                        COUNT(IF(areaid = 7 AND st IN (1,17,22,32), id, NULL)) AS AgenciasMejora_SeguimientoCliente,
                        COUNT(IF(areaid = 7 AND st IN (3,20,24,30), id, NULL)) AS AgenciasMejora_SeguimientoInterno,
                        COUNT(IF(areaid = 7 AND st IN (2,18,23,31), id, NULL)) AS AgenciasMejora_SeguimientoProveedor,
                        COUNT(IF(areaid = 7 AND st IN (6), id, NULL)) AS AgenciasMejora_ResueltoBO,
                        COUNT(IF(areaid = 7 AND st IN (27), id, NULL)) AS AgenciasMejora_ResueltoIN,
                        COUNT(IF(areaid = 7 AND st IN (5,19,25,34), id, NULL)) AS AgenciasMejora_FinBO,
                        COUNT(IF(areaid = 7 AND st IN (26,28,29,33), id, NULL)) AS AgenciasMejora_FinIN,
                        COUNT(IF(areaid = 7 AND st!=8, id, NULL)) AS AgenciasMejora_Total,

                        COUNT(IF(areaid = 9 AND st IN (8), id, NULL)) AS Afectaciones_Spam,
                        COUNT(IF(areaid = 9 AND st IN (7), id, NULL)) AS Afectaciones_Escalado,
                        COUNT(IF(areaid = 9 AND st IN (1,17,22,32), id, NULL)) AS Afectaciones_SeguimientoCliente,
                        COUNT(IF(areaid = 9 AND st IN (3,20,24,30), id, NULL)) AS Afectaciones_SeguimientoInterno,
                        COUNT(IF(areaid = 9 AND st IN (2,18,23,31), id, NULL)) AS Afectaciones_SeguimientoProveedor,
                        COUNT(IF(areaid = 9 AND st IN (6), id, NULL)) AS Afectaciones_ResueltoBO,
                        COUNT(IF(areaid = 9 AND st IN (27), id, NULL)) AS Afectaciones_ResueltoIN,
                        COUNT(IF(areaid = 9 AND st IN (5,25,19,34), id, NULL)) AS Afectaciones_FinBO,
                        COUNT(IF(areaid = 9 AND st IN (26,28,29,33), id, NULL)) AS Afectaciones_FinIN,
                        COUNT(IF(areaid = 9 AND st!=8, id, NULL)) AS Afectaciones_Total,
                        COALESCE(Mailing,0) as Mailing, COALESCE(Confirming,0) as Confirming, COALESCE(Reembolsos,0) as Reembolsos, 
                        COALESCE(MejoraContinua,0) as MejoraContinua, COALESCE(AgenciasConfirming,0) as AgenciasConfirming, COALESCE(Afectaciones,0) as Afectaciones, COALESCE(AgenciasMejora,0) as AgenciasMejora
                    FROM
                        boCases a LEFT JOIN boSes b ON a.asesor=b.asesor AND CAST(date_created AS DATE)=b.Fecha
                    GROUP BY FechaBO, a.asesor");
                    $this->db->query("ALTER TABLE boCaseSum ADD PRIMARY KEY (FechaBO, asesorBO)");
                                    
                    $this->db->query("DROP TEMPORARY TABLE IF EXISTS byDay");
                    $this->db->query("CREATE TEMPORARY TABLE byDay SELECT 
                        a.*, Exceed, Sesion, PNP+c.Comida as PNP, Sesion-PNP as Ut, FA, RTA, RTB, e.*
                    FROM
                        graf_dailySale a LEFT JOIN dep_asesores b ON a.asesor=b.asesor AND a.Fecha=b.Fecha
                        LEFT JOIN pausas c ON a.asesor=c.asesor AND a.Fecha=c.Fecha
                        LEFT JOIN pya d ON a.asesor=d.asesor AND a.Fecha=d.Fecha
                        LEFT JOIN boCaseSum e ON a.asesor=asesorBO AND a.Fecha=FechaBO
                    WHERE
                        a.Fecha BETWEEN @inicio AND @fin
                            AND a.dep = @skill AND puesto!=11");
                    
                    $query = "SELECT 
                        Fecha,
                        NOMBREASESOR(asesor,2) as asesor,
                        NOMBREASESOR(supMes,2) as supervisor, Confirming_Spam,Confirming_Escalado,Confirming_SeguimientoCliente,Confirming_SeguimientoInterno,Confirming_SeguimientoProveedor,Confirming_ResueltoBO,
                        Confirming_ResueltoIN,Confirming_FinBO,Confirming_FinIN,Confirming_Total,Mailing_Spam,Mailing_Escalado,Mailing_SeguimientoCliente,
                        Mailing_SeguimientoInterno,Mailing_SeguimientoProveedor,Mailing_ResueltoBO,Mailing_ResueltoIN,Mailing_FinBO,Mailing_FinIN,Mailing_Total,
                        MC_Spam,MC_Escalado,MC_SeguimientoCliente,MC_SeguimientoInterno,MC_SeguimientoProveedor,MC_ResueltoBO,MC_ResueltoIN,MC_FinBO,MC_FinIN,MC_Total,
                        Reembolsos_Spam,Reembolsos_Escalado,Reembolsos_SeguimientoCliente,Reembolsos_SeguimientoInterno,Reembolsos_SeguimientoProveedor,Reembolsos_ResueltoBO,
                        Reembolsos_ResueltoIN,Reembolsos_FinBO,Reembolsos_FinIN,Reembolsos_Total,AgenciasConfirming_Spam,AgenciasConfirming_Escalado,
                        AgenciasConfirming_SeguimientoCliente,AgenciasConfirming_SeguimientoInterno,AgenciasConfirming_SeguimientoProveedor,AgenciasConfirming_ResueltoBO,
                        AgenciasConfirming_ResueltoIN,AgenciasConfirming_FinBO,AgenciasConfirming_FinIN,AgenciasConfirming_Total,AgenciasMejora_Spam,AgenciasMejora_Escalado,
                        AgenciasMejora_SeguimientoCliente,AgenciasMejora_SeguimientoInterno,AgenciasMejora_SeguimientoProveedor,AgenciasMejora_ResueltoBO,AgenciasMejora_ResueltoIN,
                        AgenciasMejora_FinBO,AgenciasMejora_FinIN,AgenciasMejora_Total,Afectaciones_Spam,Afectaciones_Escalado,Afectaciones_SeguimientoCliente,
                        Afectaciones_SeguimientoInterno,Afectaciones_SeguimientoProveedor,Afectaciones_ResueltoBO,Afectaciones_ResueltoIN,Afectaciones_FinBO,Afectaciones_FinIN,
                        Afectaciones_Total,Mailing,Confirming,Reembolsos,MejoraContinua,AgenciasConfirming,Afectaciones,AgenciasMejora,
                        callsIn,
                        TTIn,
                        AHTIn,
                        callsOut,
                        TTOut,
                        AHTOut,
                        intentosOut,
                        Exceed as pausasExcedidas, PNP, Sesion, Ut,
                        FA, RTA, RTB
                    FROM
                        byDay";
                    
                    if($q = $this->db->query("CREATE TEMPORARY TABLE cuartiles $query")){
                
                        $query = "SELECT asesor, supervisor,
                        SUM(Confirming_Spam) as Confirming_Spam, SUM(Confirming_Escalado) as Confirming_Escalado, 
                        SUM(Confirming_SeguimientoCliente) as Confirming_SeguimientoCliente, SUM(Confirming_SeguimientoInterno) as Confirming_SeguimientoInterno, 
                        SUM(Confirming_SeguimientoProveedor) as Confirming_SeguimientoProveedor, SUM(Confirming_ResueltoBO) as Confirming_ResueltoBO, 
                        SUM(Confirming_ResueltoIN) as Confirming_ResueltoIN, SUM(Confirming_FinBO) as Confirming_FinBO, SUM(Confirming_FinIN) as Confirming_FinIN, 
                        SUM(Confirming_Total) as Confirming_Total, SUM(Mailing_Spam) as Mailing_Spam, SUM(Mailing_Escalado) as Mailing_Escalado, 
                        SUM(Mailing_SeguimientoCliente) as Mailing_SeguimientoCliente, SUM(Mailing_SeguimientoInterno) as Mailing_SeguimientoInterno, 
                        SUM(Mailing_SeguimientoProveedor) as Mailing_SeguimientoProveedor, SUM(Mailing_ResueltoBO) as Mailing_ResueltoBO, SUM(Mailing_ResueltoIN) as Mailing_ResueltoIN, 
                        SUM(Mailing_FinBO) as Mailing_FinBO, SUM(Mailing_FinIN) as Mailing_FinIN, SUM(Mailing_Total) as Mailing_Total, SUM(MC_Spam) as MC_Spam, 
                        SUM(MC_Escalado) as MC_Escalado, SUM(MC_SeguimientoCliente) as MC_SeguimientoCliente, SUM(MC_SeguimientoInterno) as MC_SeguimientoInterno, 
                        SUM(MC_SeguimientoProveedor) as MC_SeguimientoProveedor, SUM(MC_ResueltoBO) as MC_ResueltoBO, SUM(MC_ResueltoIN) as MC_ResueltoIN, 
                        SUM(MC_FinBO) as MC_FinBO, SUM(MC_FinIN) as MC_FinIN, SUM(MC_Total) as MC_Total, SUM(Reembolsos_Spam) as Reembolsos_Spam, 
                        SUM(Reembolsos_Escalado) as Reembolsos_Escalado, SUM(Reembolsos_SeguimientoCliente) as Reembolsos_SeguimientoCliente, 
                        SUM(Reembolsos_SeguimientoInterno) as Reembolsos_SeguimientoInterno, SUM(Reembolsos_SeguimientoProveedor) as Reembolsos_SeguimientoProveedor, 
                        SUM(Reembolsos_ResueltoBO) as Reembolsos_ResueltoBO, SUM(Reembolsos_ResueltoIN) as Reembolsos_ResueltoIN, SUM(Reembolsos_FinBO) as Reembolsos_FinBO, 
                        SUM(Reembolsos_FinIN) as Reembolsos_FinIN, SUM(Reembolsos_Total) as Reembolsos_Total, SUM(AgenciasConfirming_Spam) as AgenciasConfirming_Spam, 
                        SUM(AgenciasConfirming_Escalado) as AgenciasConfirming_Escalado, SUM(AgenciasConfirming_SeguimientoCliente) as AgenciasConfirming_SeguimientoCliente, 
                        SUM(AgenciasConfirming_SeguimientoInterno) as AgenciasConfirming_SeguimientoInterno, 
                        SUM(AgenciasConfirming_SeguimientoProveedor) as AgenciasConfirming_SeguimientoProveedor, SUM(AgenciasConfirming_ResueltoBO) as AgenciasConfirming_ResueltoBO, 
                        SUM(AgenciasConfirming_ResueltoIN) as AgenciasConfirming_ResueltoIN, SUM(AgenciasConfirming_FinBO) as AgenciasConfirming_FinBO, 
                        SUM(AgenciasConfirming_FinIN) as AgenciasConfirming_FinIN, SUM(AgenciasConfirming_Total) as AgenciasConfirming_Total, 
                        SUM(AgenciasMejora_Spam) as AgenciasMejora_Spam, SUM(AgenciasMejora_Escalado) as AgenciasMejora_Escalado, 
                        SUM(AgenciasMejora_SeguimientoCliente) as AgenciasMejora_SeguimientoCliente, SUM(AgenciasMejora_SeguimientoInterno) as AgenciasMejora_SeguimientoInterno, 
                        SUM(AgenciasMejora_SeguimientoProveedor) as AgenciasMejora_SeguimientoProveedor, SUM(AgenciasMejora_ResueltoBO) as AgenciasMejora_ResueltoBO, 
                        SUM(AgenciasMejora_ResueltoIN) as AgenciasMejora_ResueltoIN, SUM(AgenciasMejora_FinBO) as AgenciasMejora_FinBO, SUM(AgenciasMejora_FinIN) as AgenciasMejora_FinIN, 
                        SUM(AgenciasMejora_Total) as AgenciasMejora_Total, SUM(Afectaciones_Spam) as Afectaciones_Spam, SUM(Afectaciones_Escalado) as Afectaciones_Escalado, 
                        SUM(Afectaciones_SeguimientoCliente) as Afectaciones_SeguimientoCliente, SUM(Afectaciones_SeguimientoInterno) as Afectaciones_SeguimientoInterno, 
                        SUM(Afectaciones_SeguimientoProveedor) as Afectaciones_SeguimientoProveedor, SUM(Afectaciones_ResueltoBO) as Afectaciones_ResueltoBO, 
                        SUM(Afectaciones_ResueltoIN) as Afectaciones_ResueltoIN, SUM(Afectaciones_FinBO) as Afectaciones_FinBO, SUM(Afectaciones_FinIN) as Afectaciones_FinIN, 
                        SUM(Afectaciones_Total) as Afectaciones_Total, SUM(Mailing) as Mailing, SUM(Confirming) as Confirming, SUM(Reembolsos) as Reembolsos, 
                        SUM(MejoraContinua) as MejoraContinua, SUM(AgenciasConfirming) as AgenciasConfirming, SUM(Afectaciones) as Afectaciones, SUM(AgenciasMejora) as AgenciasMejora,
                        SUM(callsIn) as callsIn,
                        SUM(TTIn) as TTIn,
                        SUM(AHTIn) as AHTIn,
                        SUM(callsOut) as callsOut,
                        SUM(TTOut) as TTOut,
                        SUM(AHTOut) as AHTOut,
                        SUM(intentosOut) as intentosOut,
                        SUM(pausasExcedidas) as pausasExcedidas,
                        SUM(PNP) as PNP,
                        SUM(Ut) as Ut,
                        SUM(Sesion) as Sesion,
                        SUM(FA) as FA, SUM(RTA) as RTA, SUM(RTB) as RTB
                        FROM
                            cuartiles
                        GROUP BY asesor";

                        if($q = $this->db->query($query)){

                            $ses = $this->db->query("SELECT AVG(COALESCE(Sesion,0))*0.7 as Ses FROM cuartiles");
                            $all = $this->db->query("SELECT * FROM cuartiles");
                            $s = $ses->row_array();
                            okResponse( 'Data Obtenida', 'data', $q->result_array(), $this, 'meta', array('avgSes' => $s['Ses'], 'raw' => $all->result_array()) );
                        }else{

                            errResponse('Error en consolidación de tabla', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());

                        }

                    }else{

                        errResponse('Error al construir tabla', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());

                    }
                                    
                                    
                }
            // =================================================
            // END BO
            // =================================================
            
            // =================================================
            // START INBOUND
            // =================================================
                if( !$isBo ){


                    $this->db->query("DROP TEMPORARY TABLE IF EXISTS byDay");
                    $this->db->query("CREATE TEMPORARY TABLE byDay SELECT 
                        a.*, Exceed, Sesion, PNP+c.Comida as PNP, Sesion-PNP as Ut, FA, RTA, RTB
                    FROM
                        graf_dailySale a LEFT JOIN dep_asesores b ON a.asesor=b.asesor AND a.Fecha=b.Fecha
                        LEFT JOIN pausas c ON a.asesor=c.asesor AND a.Fecha=c.Fecha
                        LEFT JOIN pya d ON a.asesor=d.asesor AND a.Fecha=d.Fecha
                    WHERE
                        a.Fecha BETWEEN @inicio AND @fin
                            AND a.dep = @skill AND puesto!=11");
                    
                    $query = "SELECT 
                        Fecha,
                        NOMBREASESOR(asesor,2) as asesor,
                        NOMBREASESOR(supMes,2) as supervisor,
                        Monto$sv as Monto,
                        Margen$sv as Margen,
                        LocsIn,
                        LocsNotIn,
                        MontoIn$sv as Monto_In, MontoNotIn$sv as Monto_NotIn,  
                        Hotel".$paq."In"."$sv as Hotel_In, Hotel".$paq."NotIn"."$sv as Hotel_NotIn, Hotel".$paq."In"."$sv + Hotel".$paq."NotIn"."$sv as Hotel_All,
                        Vuelo".$paq."In"."$sv as Vuelo_In, Vuelo".$paq."NotIn"."$sv as Vuelo_NotIn, Vuelo".$paq."In"."$sv + Vuelo".$paq."NotIn"."$sv as Vuelo_All,
                        Transfer".$paq."In"."$sv as Transfer_In, Transfer".$paq."NotIn"."$sv as Transfer_NotIn, Transfer".$paq."In"."$sv + Transfer".$paq."NotIn"."$sv as Transfer_All,
                        Tour".$paq."In"."$sv as Tour_In, Tour".$paq."NotIn"."$sv as Tour_NotIn, Tour".$paq."In"."$sv + Tour".$paq."NotIn"."$sv as Tour_All,
                        Crucero".$paq."In"."$sv as Crucero_In, Crucero".$paq."NotIn"."$sv as Crucero_NotIn, Crucero".$paq."In"."$sv + Crucero".$paq."NotIn"."$sv as Crucero_All,";

                    IF( $p_paq ){
                        $query .= "PaqueteIn$sv as Paquete_In, PaqueteNotIn$sv as Paquete_NotIn, PaqueteIn$sv + PaqueteNotIn$sv as Paquete_All,";
                    }

                    $query .= "callsIn,
                        TTIn,
                        AHTIn,
                        callsOut,
                        TTOut,
                        AHTOut,
                        intentosOut,
                        Exceed as pausasExcedidas, PNP, Sesion, Ut,
                        FA, RTA, RTB
                    FROM
                        byDay";
                    
                    if($q = $this->db->query("CREATE TEMPORARY TABLE cuartiles $query")){
                
                        $query = "SELECT asesor, supervisor, SUM(Monto) as Monto,
                        SUM(Margen) as Margen,
                        SUM(LocsIn) as LocsIn,
                        SUM(LocsNotIn) as LocsNotIn,
                        SUM(Monto_In) as Monto_In,
                        SUM(Monto_NotIn) as Monto_NotIn,
                        SUM(Hotel_In) as Hotel_In,
                        SUM(Hotel_NotIn) as Hotel_NotIn,
                        SUM(Hotel_All) as Hotel_All,
                        SUM(Vuelo_In) as Vuelo_In,
                        SUM(Vuelo_NotIn) as Vuelo_NotIn,
                        SUM(Vuelo_All) as Vuelo_All,
                        SUM(Transfer_In) as Transfer_In,
                        SUM(Transfer_NotIn) as Transfer_NotIn,
                        SUM(Transfer_All) as Transfer_All,
                        SUM(Tour_In) as Tour_In,
                        SUM(Tour_NotIn) as Tour_NotIn,
                        SUM(Tour_All) as Tour_All,
                        SUM(Crucero_In) as Crucero_In,
                        SUM(Crucero_NotIn) as Crucero_NotIn,
                        SUM(Crucero_All) as Crucero_All,";

                        IF( boolVal($p_paq) == 1 ){
                            $query .= "SUM(Paquete_In) as Paquete_In,
                                    SUM(Paquete_NotIn) as Paquete_NotIn,
                                    SUM(Paquete_All) as Paquete_All,";
                        }

                        $query .= "SUM(callsIn) as callsIn,
                                    SUM(TTIn) as TTIn,
                                    SUM(AHTIn) as AHTIn,
                                    SUM(callsOut) as callsOut,
                                    SUM(TTOut) as TTOut,
                                    SUM(AHTOut) as AHTOut,
                                    SUM(intentosOut) as intentosOut,
                                    SUM(pausasExcedidas) as pausasExcedidas,
                                    SUM(PNP) as PNP,
                                    SUM(Ut) as Ut,
                                    SUM(Sesion) as Sesion,
                                    SUM(FA) as FA, SUM(RTA) as RTA, SUM(RTB) as RTB
                                    FROM
                                        cuartiles
                                    GROUP BY asesor";

                        if($q = $this->db->query($query)){
                            $ses = $this->db->query("SELECT AVG(COALESCE(Sesion,0))*0.7 as Ses FROM cuartiles");
                            $all = $this->db->query("SELECT * FROM cuartiles");
                            $s = $ses->row_array();
                            okResponse( 'Data Obtenida', 'data', $q->result_array(), $this, 'meta', array('avgSes' => $s['Ses'], 'raw' => $all->result_array()) );
                        }else{

                            errResponse('Error en consolidación de tabla', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());

                        }

                    }else{

                        errResponse('Error al construir tabla', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());

                    }
                }
            // =================================================
            // END INBOUND
            // =================================================
 
          return true;

        });

        jsonPrint( $result );

    }
    
    public function pcrcs_get(){
        $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

            $query = "SELECT DISTINCT
                        dep, Departamento, isBO
                    FROM
                        graf_dailySale a
                            LEFT JOIN
                        PCRCs b ON a.dep = b.id";
            

          if($q = $this->db->query($query)){
              okResponse( 'Data Obtenida', 'data', $q->result_array(), $this );
          }else{
            errResponse('Error en la base de datos', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
          }

          return true;

        });

        jsonPrint( $result );
        
    }
    
    

}
