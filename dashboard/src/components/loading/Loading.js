import React from 'react'
import { Spin } from 'antd';
import './Loading.css'

export default ()=>{
    return (<div class="loadingWrapper"><div class="loading"><Spin size="large"  /></div></div>)
}