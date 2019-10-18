// src/App.js

import React from "react";
import NavBar from "./components/NavBar";

// New - import the React Router components, and the Profile page component
import { BrowserRouter, Route, Switch } from "react-router-dom";
import Profile from "./views/Profile";
import Collections from "./views/collections/Collections";
import Layout from './components/layout/Layout';
import Indexes from "./views/indexes/Indexes";
import Documents from "./views/documents/Documents";

function App() {
  return (
    <div className="App">
      <BrowserRouter>
      <Layout>
      {/* New - use BrowserRouter to provide access to /profile */}
        <header>
          {/* <NavBar /> */}
          <div style={{display: "none"}}>{process.env.NODE_ENV} {process.env.REACT_APP_API_BASE_PATH}</div>
        </header>
        <Switch>
          <Route path="/" exact />
          <Route path="/profile" component={Profile} />
          <Route path="/collections" component={Collections} />
          <Route path="/indexes/:collection" component={Indexes} />
          <Route path="/documents/:collection" component={Documents} />
        </Switch>

      </Layout>
      </BrowserRouter>
    </div>
  );
}

export default App;