<%@ page language="java" contentType="text/html; charset=UTF-8"
    pageEncoding="UTF-8" isErrorPage="true" %>
<!DOCTYPE HTML>
<html>
<head>
<meta charset="utf-8">
<meta name="renderer" content="webkit|ie-comp|ie-stand">
<meta http-equiv="X-UA-Compatible" content="IE=edge,chrome=1">
<meta name="viewport" content="width=device-width,initial-scale=1,minimum-scale=1.0,maximum-scale=1.0,user-scalable=no" />
<meta http-equiv="Cache-Control" content="no-siteapp" />

<style>
*{padding:0px; margin:0px; font-family:"Microsoft YaHei";}
img{border:0px; border-color:#fff; }
.img404box{height:263px; margin-top:10%; text-align:center; border-bottom:solid #2db7f5 1px; }
.txt{text-align:center; color:#2db7f5; padding-top:35px;}
.txt a{background:#2db7f5; padding:6px 35px; border-radius:6px; color:#fff;  margin-left:30px;}
</style>
<title>404</title>
<meta name="keywords" content="">
<meta name="description" content="">
</head>
<body>
<div class="img404box"><img  src="<%=request.getContextPath() %>/404/images/404.png"></div>
<div class="txt">页面找不到了 <a href="javascript:window.history.back();">返回</a></div>
<div></div>
<div></div>
</body>
</html>