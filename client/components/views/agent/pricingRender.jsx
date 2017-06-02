import React, { Component } from 'react'
import {Link} from 'react-router-dom'
import { StyleSheet, css } from 'aphrodite'
import styles from './styles/pricing'
import checkBlue from '../../../assets/svg/iconCheckBlue.svg'
import checkRed from '../../../assets/svg/iconCheckRed.svg'
import plusBlue from '../../../assets/svg/iconPlusBlue.svg'
import plusRed from '../../../assets/svg/iconPlusRed.svg'

const PlanRender = (props) => {
  console.log('pr', props.planSpecifics)
  const getIcon = (icon, color) => {
    console.log('ic',icon)
    console.log('co',color)
    if ((icon === "check") && (color === "blue")){
      return checkBlue
    } else if ((icon === "check") && (color === "red")){
      return checkRed
    } else if ((icon === "plus") && (color === "blue")){
      return plusBlue
    } else {
      return plusRed
    }
  }

  const isSelected = (truthy) => {
    if(truthy){
      return <Link to="/pricing" className={css(styles.changeLink)}>Change Plan</Link>
    }
  }

  return (
    <div>
      <h3 className={css(styles.planTitle)}>{props.planSpecifics.title} {isSelected(props.selected)}</h3>
      <div className={css(styles.details)}>
        <h3 className={css(styles.planPrice)}>
          <span className={css(styles.dollarSign)}>$</span>
          <span className={css(styles.price)}>{props.planSpecifics.price}</span>
          <span className={css(styles.cents)}>.00</span>
        </h3>
        <h4 className={css(styles.desc)}>{props.planSpecifics.desc}</h4>
        <ul className={css(styles.features)}>
          {props.planSpecifics.features.map((feat, i) => {
            return (
              <li key={i} className={css(styles.feature)}>
                <img
                  className={css(styles.icon)}
                  src={`/${getIcon(feat.icon, props.planSpecifics.color)}`}
                  alt={`${feat.icon} ${props.planSpecifics.color}`}
                />
                <p className={css(styles.featureDesc)}>{feat.desc}</p>
              </li>
            )
          })}
        </ul>
      </div>
    </div>
  )
}

export default PlanRender
