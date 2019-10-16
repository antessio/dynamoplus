import React from 'react'
//import { Row, Col } from 'antd';
import { Tag } from 'antd';

import { Descriptions } from 'antd';

export default (props)=>{
    const index = props.index
    const indexName = index.name;
    const indexNameSplit1 = indexName.split("__ORDER_BY__")
    const fields = indexNameSplit1[0].split("__")
    const orderingKey = indexNameSplit1.length >1?indexNameSplit1[1]:null;

    return (
            <Descriptions bordered 
            column={{ xxl: 4, xl: 3, lg: 3, md: 3, sm: 2, xs: 1 }}
            title={indexName}>
                <Descriptions.Item label={"Fields"}>
                {fields.map(f=>(
                    <div key={f}>
                        <Tag>
                            {f}
                        </Tag><br/>
                        </div>
                ))}
                </Descriptions.Item>    
                {orderingKey && <Descriptions.Item  label={"Ordering key"}>{orderingKey}</Descriptions.Item>    }
          </Descriptions>
    )
}