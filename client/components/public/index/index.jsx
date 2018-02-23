// @flow
import React, { Component } from 'react'
import { BrowserRouter as Router, Route } from 'react-router-dom'
import { StyleSheet, css } from 'aphrodite'
import styles from '../../../styles/common_layout'
import Hero from './hero'
import FeaturesBenefits from './features_benefits'
import BirdsEyeView from './birds_eye_view'
import Photos from './photos'
import WhatYouGet from './what_you_get'
import SampleVideo from './sample_video'
import PricingList from '../agent/pricing_list'
import Share from './share'
import OurClientsLoveUs from './our_clients_love_us'

const Index = (props: Object) => {
  return(
    <div className={css(styles.centerMainContent)}>
      <div>
        <Hero {...props} />
        <FeaturesBenefits />
        <BirdsEyeView />
        {/* <Photos /> */}
        {/* <OurClientsLoveUs />
        <Share /> */}
      </div>
    </div>
  )
}

export default Index