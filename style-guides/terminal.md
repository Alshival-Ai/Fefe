# HTML 

```
<div class="container">
  <div class="terminal_toolbar">
    <div class="butt">
      <button class="btn btn-color"></button>
      <button class="btn"></button>
      <button class="btn"></button>
    </div>
    <p class="user">johndoe@admin: ~</p>
    <button class="add_tab">+</button>
  </div>
  <div class="terminal_body">
    <div class="terminal_promt">
      <span class="terminal_user">johndoe@admin:</span>
      <span class="terminal_location">~</span>
      <span class="terminal_bling">$</span>
      <span class="terminal_cursor"></span>
    </div>
  </div>
</div>
```

# CSS 

```
/* From Uiverse.io by mahiatlinux */ 
.container {
  width: 300px;
  height: 300px;
  background: #1e1e1e;
  border-radius: 10px;
  box-shadow: 0 10px 30px rgba(0, 0, 0, 0.3);
  overflow: hidden;
}

.terminal_toolbar {
  display: flex;
  height: 35px;
  align-items: center;
  padding: 0 15px;
  background: #2d2d2d;
  justify-content: space-between;
}

.butt {
  display: flex;
  align-items: center;
}

.btn {
  height: 13px;
  width: 13px;
  border-radius: 50%;
  margin-right: 8px;
  border: none;
  cursor: pointer;
  transition: transform 0.2s ease;
}

.btn:hover {
  transform: scale(1.1);
}

.btn-color:nth-child(1) {
  background: #ff5f56;
}
.btn-color:nth-child(2) {
  background: #ffbd2e;
}
.btn-color:nth-child(3) {
  background: #27c93f;
}

.add_tab {
  border: none;
  color: #ffffff;
  background: #3a3a3a;
  padding: 5px 10px;
  border-radius: 5px;
  font-size: 12px;
  cursor: pointer;
  transition: background 0.2s ease;
}

.add_tab:hover {
  background: #4a4a4a;
}

.user {
  color: #ffffff;
  font-size: 14px;
  font-weight: bold;
}

.terminal_body {
  background: #1e1e1e;
  height: calc(100% - 35px);
  padding: 15px;
  font-family: "Consolas", monospace;
  font-size: 14px;
  line-height: 1.5;
  overflow-y: auto;
}

.terminal_promt {
  display: flex;
  align-items: center;
  margin-bottom: 10px;
}

.terminal_promt span {
  margin-right: 5px;
}

.terminal_user {
  color: #00ff9c;
}
.terminal_location {
  color: #0066ff;
}
.terminal_bling {
  color: #ff00ff;
}

.terminal_cursor {
  display: inline-block;
  width: 8px;
  height: 18px;
  background: #ffffff;
  animation: blink 1s step-end infinite;
}

@keyframes blink {
  0%,
  100% {
    opacity: 1;
  }
  50% {
    opacity: 0;
  }
}

```

# React 
This snippet is using styled-components. Install it with npm i styled-components or yarn add styled-components, or copy the styles to your own CSS file for this code to work.
```
import React from 'react';
import styled from 'styled-components';

const Card = () => {
  return (
    <StyledWrapper>
      <div className="container">
        <div className="terminal_toolbar">
          <div className="butt">
            <button className="btn btn-color" />
            <button className="btn" />
            <button className="btn" />
          </div>
          <p className="user">johndoe@admin: ~</p>
          <button className="add_tab">+</button>
        </div>
        <div className="terminal_body">
          <div className="terminal_promt">
            <span className="terminal_user">johndoe@admin:</span>
            <span className="terminal_location">~</span>
            <span className="terminal_bling">$</span>
            <span className="terminal_cursor" />
          </div>
        </div>
      </div>
    </StyledWrapper>
  );
}

const StyledWrapper = styled.div`
  .container {
    width: 300px;
    height: 300px;
    background: #1e1e1e;
    border-radius: 10px;
    box-shadow: 0 10px 30px rgba(0, 0, 0, 0.3);
    overflow: hidden;
  }

  .terminal_toolbar {
    display: flex;
    height: 35px;
    align-items: center;
    padding: 0 15px;
    background: #2d2d2d;
    justify-content: space-between;
  }

  .butt {
    display: flex;
    align-items: center;
  }

  .btn {
    height: 13px;
    width: 13px;
    border-radius: 50%;
    margin-right: 8px;
    border: none;
    cursor: pointer;
    transition: transform 0.2s ease;
  }

  .btn:hover {
    transform: scale(1.1);
  }

  .btn-color:nth-child(1) {
    background: #ff5f56;
  }
  .btn-color:nth-child(2) {
    background: #ffbd2e;
  }
  .btn-color:nth-child(3) {
    background: #27c93f;
  }

  .add_tab {
    border: none;
    color: #ffffff;
    background: #3a3a3a;
    padding: 5px 10px;
    border-radius: 5px;
    font-size: 12px;
    cursor: pointer;
    transition: background 0.2s ease;
  }

  .add_tab:hover {
    background: #4a4a4a;
  }

  .user {
    color: #ffffff;
    font-size: 14px;
    font-weight: bold;
  }

  .terminal_body {
    background: #1e1e1e;
    height: calc(100% - 35px);
    padding: 15px;
    font-family: "Consolas", monospace;
    font-size: 14px;
    line-height: 1.5;
    overflow-y: auto;
  }

  .terminal_promt {
    display: flex;
    align-items: center;
    margin-bottom: 10px;
  }

  .terminal_promt span {
    margin-right: 5px;
  }

  .terminal_user {
    color: #00ff9c;
  }
  .terminal_location {
    color: #0066ff;
  }
  .terminal_bling {
    color: #ff00ff;
  }

  .terminal_cursor {
    display: inline-block;
    width: 8px;
    height: 18px;
    background: #ffffff;
    animation: blink 1s step-end infinite;
  }

  @keyframes blink {
    0%,
    100% {
      opacity: 1;
    }
    50% {
      opacity: 0;
    }
  }`;

export default Card;
```