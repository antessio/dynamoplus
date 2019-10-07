// pages/index.js
import React from 'react'
import Default from '../layouts/default'
import axios from 'axios'
const meta = { title: 'Index title', description: 'Index description' }

class IndexPage extends React.Component {
  constructor (props) {
    super(props)
    this.state = {
        }
    }
  render () {
    return (<Default meta={meta}>
        <div>
          <h1>Dynamoplus dashboard</h1>
        </div>
      </Default>
    )
  }
}

export default IndexPage
