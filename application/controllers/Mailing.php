<?php
//defined('BASEPATH') OR exit('No direct script access allowed');
require( APPPATH.'/libraries/REST_Controller.php');
// use REST_Controller;


class Mailing extends REST_Controller {

  public function __construct(){

    parent::__construct();
    $this->load->helper('json_utilities');
    $this->load->helper('validators');
    $this->load->helper('jwt');
    $this->load->helper('mailing');
    $this->load->database();
  }

    private function countReturn($q, $msg, $err = false){
        if( $q->num_rows() == 0 ){
            if( $err ){
                errResponse($msg, REST_Controller::HTTP_BAD_REQUEST, $this, 'error', false);
            }else{
                okResponse($msg, 'data', true, $this);
            }
        }
    }
    
    private function sendMail( $titulo, $user, $tipo, $body ){
        $msg = mailingV2::msg_encapsule($titulo, $body);
            
        if( mailingV2::send($user, $titulo, $msg) ){
            $this->db->query("INSERT INTO mail_dailyCheck VALUES (NULL, '$tipo', CURDATE(), '$user', 1, NULL) ON DUPLICATE KEY UPDATE sent = 1");    
        }else{
            $this->db->query("INSERT INTO mail_dailyCheck VALUES (NULL, '$tipo', CURDATE(), '$user', 0, NULL) ON DUPLICATE KEY UPDATE sent = 0");
        }
    }
    
    private function getMailList( $tipo ){
        $mailQ = $this->db->query("SELECT 
                                        a.*, NOMBREASESOR(asesor_id, 1) AS Nombre, sent
                                    FROM
                                        mail_lists a
                                            LEFT JOIN
                                        userDB b ON a.usuario = b.username
                                            LEFT JOIN
                                        mail_dailyCheck c ON a.usuario = c.user
                                            AND c.Fecha = CURDATE()
                                            AND a.notif = c.tipo
                                    WHERE
                                        notif = '$tipo'
                                            AND COALESCE(sent, 0) = 0");
        
        $this->countReturn($mailQ, 'Sin mails pendientes');
        return $mailQ->result_array();
        
    }

    public function contratosVencidos_get(){
        
        $mails = $this->getMailList('contratos');

        $contQ = $this->db->query("SELECT 
                                a.asesor,
                                NOMBREASESOR(a.asesor, 2) AS Nombre,
                                NOMBREDEP(dep) AS Dep,
                                fin,
                                CASE
                                    WHEN CURDATE() > fin THEN 'Vencido'
                                    WHEN fin < ADDDATE(CURDATE(), 7) THEN 'Por Vencer'
                                    ELSE NULL
                                END AS st
                            FROM
                                asesores_contratos a
                                    LEFT JOIN
                                dep_asesores b ON a.asesor = b.asesor
                                    AND CURDATE() = b.Fecha
                            WHERE
                                activo = 1 AND deleted = 0 AND tipo = 1
                                    AND vacante IS NOT NULL
                                    AND dep != 29
                            HAVING st IS NOT NULL
                            ORDER BY st, Nombre");
        
        $contratos = $contQ->result_array();
        
        $this->countReturn($contQ, 'Sin contratos por reportar');
        
        $titulo = "Contratos por vencidos y por vencer CC";
        
        $body = "<div><table style='text-align: left'>\n";
        
        foreach($contratos as $index => $info){
            $color = $info['st'] == 'Vencido' ? 'red' : '#c6b64b';
            $body .= "<tr><td style='padding: 5px; border: 1px solid #d5d3d3;'>".$info['Nombre']."</td><td style='padding: 5px; border: 1px solid #d5d3d3;'>".$info['Dep']."</td><td style='padding: 5px; border: 1px solid #d5d3d3;'><span style='color: $color'>".$info['fin']."</span></td><td style='padding: 5px; border: 1px solid #d5d3d3;'><span style='color: $color'>".$info['st']."</span></td><td style='padding: 5px; border: 1px solid #d5d3d3;'><a href='https://operaciones.pricetravel.com.mx/cycv2/#/detail-asesor/".$info['asesor']."'>Ver en CyC</a></td></tr>\n";
        }
        unset($index, $info);
        
        $body .= "</table></div><br>\n";

        
        
        foreach( $mails as $index => $info ){
            $text = '';
            $text = "<p>Hola ".$info['Nombre'].",</p><p>Estos son los contratos vencidos y próximos a vencer:</p>".$body;
            $this->sendMail($titulo, $info['usuario'], 'contratos', $text);
        }
        
        okResponse('Revisión de contratos exitosa', 'data', true, $this);
    }
    
    public function faltasConsecutivas_get(){
        
        $check = $this->db->query("SELECT IF(CAST(NOW() as TIME) > '07:00:00',1,0) as flag");
        $ch = $check->row_array();
        
        if( $ch['flag'] != 1 ){
            okResponse('Fuera de Horario', 'data', true, $this);
        }
        
        $mails = $this->getMailList('faltasConsecutivas');

        $faltasQ = $this->db->query("SELECT 
                                        a.asesor, NOMBREASESOR(a.asesor,2) as Nombre,
                                        NOMBREDEP(a.dep) as Departamento,
                                        IF(COUNT(IF(Fecha >= ADDDATE(CURDATE(), - 5)
                                                AND COALESCE(code,0) != 'D' AND (code = 'F'
                                                OR CHECKLOG(Fecha, a.asesor, 'IN') IS NULL),
                                            Fecha,
                                            NULL)) +
                                        COUNT(IF(Fecha >= ADDDATE(CURDATE(), - 5)
                                                AND code = 'D',
                                            Fecha,
                                            NULL)) = 5,1,0) AS ConsecutiveLast3,

                                        COUNT(IF(Fecha >= ADDDATE(CURDATE(), - 7)
                                                AND (code = 'F'
                                                OR (code IS NULL AND CHECKLOG(Fecha, a.asesor, 'IN')) IS NULL),
                                            Fecha,
                                            NULL)) AS Last7,
                                        COUNT(IF(Fecha >= ADDDATE(CURDATE(), - 5)
                                                AND code = 'D',
                                            Fecha,
                                            NULL)) as Descansos
                                    FROM
                                        (SELECT 
                                            asesor, dep, puesto
                                        FROM
                                            dep_asesores
                                        WHERE
                                            dep != 29 AND vacante IS NOT NULL
                                                AND Fecha = CURDATE()) a
                                            LEFT JOIN
                                        (SELECT 
                                            a.asesor, a.Fecha, IF(js=je,'D',code) as code
                                        FROM
                                            asesores_programacion a
                                        LEFT JOIN asesores_ausentismos b ON a.asesor = b.asesor
                                            AND a.Fecha = b.Fecha
                                        LEFT JOIN config_tiposAusentismos c ON b.ausentismo = c.id
                                        WHERE
                                            a.Fecha BETWEEN ADDDATE(CURDATE(), - 7) AND ADDDATE(CURDATE(), - 1)
                                                AND c.id IS NULL
                                                OR c.code LIKE 'F') b ON a.asesor = b.asesor
                                    GROUP BY a.asesor
                                    HAVING ConsecutiveLast3 = 1
                                    ORDER BY Nombre");
        
        $this->countReturn($faltasQ, 'Sin faltas por reportar');
        $faltas = $faltasQ->result_array();
        
        $titulo = "Asesores con 3 faltas o más consecutivas";
        
        $body = "<div><table style='text-align: left'>\n";
        
        foreach($faltas as $index => $info){
            $color = $info['st'] == 'Vencido' ? 'red' : '#c6b64b';
            $body .= "<tr><td style='padding: 5px; border: 1px solid #d5d3d3;'>".$info['Nombre']."</td><td style='padding: 5px; border: 1px solid #d5d3d3;'>".$info['Departamento']."</td><td style='padding: 5px; border: 1px solid #d5d3d3;'><span style='color: red'>3 Faltas Consecutivas</span></td><td style='padding: 5px; border: 1px solid #d5d3d3;'><span style='color: #2862bf'>".$info['Last7']." faltas en los últimos 7 días con ".$info['Descansos']." descansos intermedios</span></td><td style='padding: 5px; border: 1px solid #d5d3d3;'><a href='https://operaciones.pricetravel.com.mx/cycv2/#/detail-asesor/".$info['asesor']."'>Ver en CyC</a></td></tr>\n";
        }
        unset($index, $info);
        
        $body .= "</table></div><br>\n";

        
        
        foreach( $mails as $index => $info ){
            $text = '';
            $text = "<p>Hola ".$info['Nombre'].",</p><p>Estos asesores cuentan con 3 faltas consecutivas (con o sin descansos intermedios):</p>".$body;
            $this->sendMail($titulo, $info['usuario'], 'faltasConsecutivas', $text);
        }
        
        okResponse('Revisión de contratos exitosa', 'data', true, $this);
    }  
    
    public function cumpleHoy_get(){
        
        $check = $this->db->query("SELECT IF(CAST(NOW() as TIME) > '07:00:00',1,0) as flag, CURDATE() as today");
        $ch = $check->row_array();
        
        if( $ch['flag'] != 1 ){
            okResponse('Fuera de Horario', 'data', true, $this);
        }
        
        $today = $ch['today'];
        
        $mails = $this->getMailList('cumpleHoy');

        $q = $this->db->query("SELECT 
                                    asesor,
                                    NOMBREDEP(dep) AS Departamento,
                                    NOMBREASESOR(asesor, 2) AS Nombre,
                                    Fecha_Nacimiento,
                                    CONCAT(DAY(Fecha_Nacimiento),
                                            '/',
                                            MONTH(Fecha_Nacimiento)) AS Fecha,
                                    TIMESTAMPDIFF(YEAR,
                                        Fecha_Nacimiento,
                                        ADDDATE(CURDATE(), 31)) AS edad,
                                    IF(MONTH(Fecha_Nacimiento) = MONTH(CURDATE())
                                            AND DAY(Fecha_Nacimiento) = DAY(CURDATE()),
                                        1,
                                        0) AS today
                                FROM
                                    dep_asesores a
                                        LEFT JOIN
                                    Asesores b ON a.asesor = b.id
                                WHERE
                                    Fecha = CURDATE()
                                        AND vacante IS NOT NULL
                                HAVING (MONTH(Fecha_Nacimiento) = MONTH(CURDATE())
                                    AND DAY(Fecha_Nacimiento) = DAY(CURDATE()))
                                    OR (MONTH(Fecha_Nacimiento) = MONTH(ADDDATE(CURDATE(), 1))
                                    AND DAY(Fecha_Nacimiento) = DAY(ADDDATE(CURDATE(), 1)))
                                ORDER BY today DESC , Nombre");
        
        $this->countReturn($q, 'Sin cumpleaños hoy');
        $result = $q->result_array();
        
        $titulo = "Los cumpleañeros del día $today";
        
        $body = "<div><table style='text-align: left'>\n";
        
        foreach($result as $index => $info){            
                $color = $info['today'] == '1' ? 'red' : '#2862bf';
                $dia = $info['today'] == '1' ? 'Hoy' : 'Mañana';
                $body .= "<tr>
                <td style='padding: 5px; border: 1px solid #d5d3d3;'>".$info['Nombre']."</td>
                <td style='padding: 5px; border: 1px solid #d5d3d3;'>".$info['Departamento']."</td>
                <td style='padding: 5px; border: 1px solid #d5d3d3;'>".$info['edad']." años</td>
                <td style='padding: 5px; border: 1px solid #d5d3d3;'><span style='color: $color'>".$info['Fecha']."</span></td>
                <td style='padding: 5px; border: 1px solid #d5d3d3;'><span style='color: $color'><b>$dia</b></span></td>
                <td style='padding: 5px; border: 1px solid #d5d3d3;'><a href='https://operaciones.pricetravel.com.mx/cycv2/#/detail-asesor/".$info['asesor']."'>Ver en CyC</a></td>
                </tr>\n";
        }
        unset($index, $info);
        
        $body .= "</table></div><br>\n";

        
        
        foreach( $mails as $index => $info ){
            $text = '';
            $text = "<p>Hola ".$info['Nombre'].",</p><p>Estos son los cumpleañeros de hoy y mañana:</p>".$body;
            $this->sendMail($titulo, $info['usuario'], 'cumpleHoy', $text);
        }
        
        okResponse('Cumpleaños enviados', 'data', true, $this);
    }
    
    public function cumplePersonalizado_get(){
        
        $check = $this->db->query("SELECT IF(CAST(NOW() as TIME) > '07:00:00',1,0) as flag, CURDATE() as today");
        $ch = $check->row_array();
        
        if( $ch['flag'] != 1 ){
            okResponse('Fuera de Horario', 'data', true, $this);
        }
        
        $today = $ch['today'];

        $q = $this->db->query("SELECT 
                                asesor,
                                NOMBREDEP(dep) AS Departamento,
                                NOMBREASESOR(asesor, 1) AS Nombre,
                                b.Usuario,
                                Fecha_Nacimiento,
                                ADDDATE(CURDATE(), 90) AS DIALIMITE,
                                CONCAT(DAY(Fecha_Nacimiento),
                                        '/',
                                        MONTH(Fecha_Nacimiento)) AS Fecha,
                                TIMESTAMPDIFF(YEAR,
                                    Fecha_Nacimiento,
                                    ADDDATE(CURDATE(), 31)) AS edad,
                                IF(MONTH(Fecha_Nacimiento) = MONTH(CURDATE())
                                        AND DAY(Fecha_Nacimiento) = DAY(CURDATE()),
                                    1,
                                    0) AS today,
                                sent
                            FROM
                                dep_asesores a
                                    LEFT JOIN
                                Asesores b ON a.asesor = b.id
                                    LEFT JOIN
                                mail_dailyCheck c ON b.Usuario = c.user
                                    AND CURDATE() = c.Fecha
                                    AND c.tipo = 'cumplePersonalizado'
                            WHERE
                                a.Fecha = CURDATE()
                                    AND vacante IS NOT NULL
                            HAVING MONTH(Fecha_Nacimiento) = MONTH(CURDATE())
                                AND DAY(Fecha_Nacimiento) = DAY(CURDATE())
                                AND COALESCE(sent, 0) = 0
                            ORDER BY today DESC , Nombre");
        
        $this->countReturn($q, 'Sin cumpleaños por enviar');
        $result = $q->result_array();
        
        
        foreach($result as $index => $info){            
                $titulo = "Feliz Cumpleaños ".$info['Nombre'];

                $body = "<p>&nbsp;</p>
                            <table style='width: 396px; height: 500px; background-color: #3f8991; margin-left: auto; margin-right: auto;' cellspacing='2' cellpadding='2'>
                            <tbody>
                            <tr style='height: 6px;'>
                            <td style='width: 27.5px; height: 6px;'>&nbsp;</td>
                            <td style='width: 311.5px; height: 6px;'>&nbsp;</td>
                            <td style='width: 32px; height: 6px;'>&nbsp;</td>
                            </tr>
                            <tr style='height: 139px;'>
                            <td style='width: 27.5px; height: 139px;'>&nbsp;</td>
                            <td style='width: 311.5px; height: 139px; background: white'>
                            <p style='text-align: center;'>Hola ".substr($info['Nombre'], 0, strpos($info['Nombre'],' '))."!</p>
                            <p style='text-align: center;'>&nbsp;</p>
                            <p style='text-align: center;'>El equipo de <strong>WFM / GTR</strong> te desea un</p>
                            <h2 style='text-align: center;'><span style='color: #0000ff;'><strong>&iexcl;Feliz Cumplea&ntilde;os!</strong></span></h2>
                            <p style='text-align: center;'>Recuerda que puedes hacer uso de tu beneficio a partir de hoy y hasta el</p>
                            <p style='text-align: center;'><strong>".$info['DIALIMITE']."</strong></p>
                            <p style='text-align: center;'>revisando siempre la disponibilidad en el calendario de ausentismos de tu &aacute;rea.</p>
                            <p style='text-align: center;'>&nbsp;</p>
                            <p style='text-align: center;'>Ten un excelente d&iacute;a y disfrutalo al m&aacute;ximo... <span style='text-decoration: underline; color: #0000ff;'><strong>hoy s&oacute;lo se trata de t&iacute;</strong></span>&nbsp; =D</p>
                            <p style='text-align: center;'>&nbsp;</p>
                            <p style='text-align: center;'>Atte.&nbsp;El equipo de WFM</p>
                            </td>
                            <td style='width: 32px; height: 139px;'>&nbsp;</td>
                            </tr>
                            <tr style='height: 8px;'>
                            <td style='width: 27.5px; height: 8px;'>&nbsp;</td>
                            <td style='width: 311.5px; height: 8px;'>&nbsp;</td>
                            <td style='width: 32px; height: 8px;'>&nbsp;</td>
                            </tr>
                            </tbody>
                            </table>
                            <p>&nbsp;</p>
                            <p>&nbsp;</p>";
                $this->sendMail($titulo, $info['Usuario'], 'cumplePersonalizado', $body);
                $this->sendMail($titulo, 'albert.sanchez', 'cumplePersonalizado', $body);
        }
        unset($index, $info);
        
        okResponse('Cumpleaños enviados', 'data', true, $this);
    }
    
    public function cumpleMes_get(){
        
        $check = $this->db->query("SELECT IF(CAST(NOW() as TIME) > '07:00:00' AND DAY(CURDATE()) = 1 ,1,0) as flag, 
                                    CASE 
                                        WHEN MONTH(CURDATE()) = 1 THEN 'Enero'
                                        WHEN MONTH(CURDATE()) = 2 THEN 'Febrero'
                                        WHEN MONTH(CURDATE()) = 3 THEN 'Marzo'
                                        WHEN MONTH(CURDATE()) = 4 THEN 'Abril'
                                        WHEN MONTH(CURDATE()) = 5 THEN 'Mayo'
                                        WHEN MONTH(CURDATE()) = 6 THEN 'Junio'
                                        WHEN MONTH(CURDATE()) = 7 THEN 'Julio'
                                        WHEN MONTH(CURDATE()) = 8 THEN 'Agosto'
                                        WHEN MONTH(CURDATE()) = 9 THEN 'Septiembre'
                                        WHEN MONTH(CURDATE()) = 10 THEN 'Octubre'
                                        WHEN MONTH(CURDATE()) = 11 THEN 'Noviembre'
                                        WHEN MONTH(CURDATE()) = 12 THEN 'Diciembre'
                                    END as today");
        $ch = $check->row_array();
        
        if( $ch['flag'] != 1 ){
            okResponse('Fuera de Horario o no primer dia del mes', 'data', true, $this);
        }
        
        $today = $ch['today'];
        
        $mails = $this->getMailList('cumpleMes');

        $q = $this->db->query("SELECT 
                                asesor,
                                NOMBREDEP(dep) AS Departamento,
                                NOMBREASESOR(asesor, 2) AS Nombre,
                                Fecha_Nacimiento,
                                CONCAT(DAY(Fecha_Nacimiento),
                                        '/',
                                        MONTH(Fecha_Nacimiento)) AS Fecha,
                                TIMESTAMPDIFF(YEAR,
                                    Fecha_Nacimiento,
                                    ADDDATE(CURDATE(), 31)) AS edad,
                                IF(MONTH(Fecha_Nacimiento) = MONTH(CURDATE())
                                        AND DAY(Fecha_Nacimiento) = DAY(CURDATE()),
                                    1,
                                    0) AS today
                            FROM
                                dep_asesores a
                                    LEFT JOIN
                                Asesores b ON a.asesor = b.id
                            WHERE
                                Fecha = CURDATE()
                                    AND vacante IS NOT NULL
                            HAVING MONTH(Fecha_Nacimiento) = MONTH(CURDATE())
                            ORDER BY Nombre;");
        
        $this->countReturn($q, 'Sin cumpleaños en el mes');
        $result = $q->result_array();
        
        $titulo = "Los cumpleañeros del mes: $today";
        
        $body = "<div><table style='text-align: left'>\n";
        
        foreach($result as $index => $info){            
                $color = $info['today'] == '1' ? 'red' : '#2862bf';
                $dia = $info['today'] == '1' ? 'Hoy' : 'Mañana';
                $body .= "<tr>
                <td style='padding: 5px; border: 1px solid #d5d3d3;'>".$info['Nombre']."</td>
                <td style='padding: 5px; border: 1px solid #d5d3d3;'>".$info['Departamento']."</td>
                <td style='padding: 5px; border: 1px solid #d5d3d3;'>".$info['edad']." años</td>
                <td style='padding: 5px; border: 1px solid #d5d3d3;'><span style='color: $color'>".$info['Fecha']."</span></td>
                <td style='padding: 5px; border: 1px solid #d5d3d3;'><a href='https://operaciones.pricetravel.com.mx/cycv2/#/detail-asesor/".$info['asesor']."'>Ver en CyC</a></td>
                </tr>\n";
        }
        unset($index, $info);
        
        $body .= "</table></div><br>\n";

        
        
        foreach( $mails as $index => $info ){
            $text = '';
            $text = "<p>Hola ".$info['Nombre'].",</p><p>Estos son los cumpleañeros del mes $today:</p>".$body;
            $this->sendMail($titulo, $info['usuario'], 'cumpleMes', $text);
        }
        
        okResponse('Cumpleaños enviados', 'data', true, $this);
    }
    
    public function test_get(){
        $user = $this->uri->segment(3);
        $type = $this->uri->segment(4);
        
        $msg = mailingV2::sendMail($user,$type);
        
        okResponse('Fuera de Horario o no primer dia del mes', 'data', $msg, $this);
    }
    
}
